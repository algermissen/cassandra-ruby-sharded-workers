require 'cassandra'
require 'logger'

class ShardedQueue
 
  # Used for the messages we take from the queue
  Message = Struct.new(:due, :message)

  def initialize(name,host,keyspace)

    @host     = host
    @keyspace = keyspace
    @name     = name

    # The number of shards per time slice. You should have
    # at least this number of workers for all shards to be
    # processed.
    @number_of_shards = 6

    # To ensure that putting messages to the shards does not
    # interfere with the consumers taking messages up to 'now'
    # we add a safety offset that make putted messages due
    # some short time ahead of 'now'.
    @due_safety_offset_seconds = 60

    # The timepart format determines the granularity of the
    # rows. The shard_granularity_in_seconds property must
    # match that granularity because it is used to advance
    # a consumer to the next row when a given row has
    # been exhausted.
    #
    # Example combinations would be:
    #
    # - "%Y%j%H%M" and 60
    # - "%Y%j%H"   and 3660
    # - "%Y%j"     and 86400
    #
    @timepart_format = "%Y%j%H%M"
    @shard_granularity_in_seconds = 60

    # To remove all messages from the queue after some time
    # we set a TTL on each message that is large enough to
    # be outside any useful timing, here 30 days. 
    @message_ttl = 30 * 86400

    @logger = Logger.new(STDERR)
    @logger.level = Logger::DEBUG

    @timeUuidGenerator = Cassandra::TimeUuid::Generator.new
    
    @cluster = Cassandra.connect(hosts: [@host],consistency: :quorum)
    @session = @cluster.connect(keyspace)
    @logger.info("Cassandra connection established and got session")

    @session.execute(<<-CREATETABLE
    create table if not exists shards (
      name text,
      shard int,
      lock text,  
      taken timestamp,
      touched timestamp,
      last timeuuid,
      primary key (name,shard)
    )
    with clustering order by (shard asc)
    CREATETABLE
    )

    @session.execute(<<-CREATETABLE
    create table if not exists work (
      name text,
      timepart bigint,
      shard bigint,
      due timeuuid,
      message text,
      primary key ((name,timepart,shard),due)
     )
     with clustering order by (due asc)
    CREATETABLE
    )

    @init_shard_stmt = @session.prepare("insert into shards (name,shard,last) values (?,?,now())")

    @put_stmt = @session.prepare("insert into work (name,timepart,shard,due,message) values (?,?,?,?,?) using ttl ?")
    @lock_shard_stmt = @session.prepare("update shards using ttl ? set lock = ?, touched = dateOf(now()) where name = ? and shard = ? if lock = null")
    @touch_lock_stmt = @session.prepare("update shards using ttl ? set lock = ?, touched = dateOf(now()) where name = ? and shard = ?")

    @take_stmt = @session.prepare("select due,message from work where name = ? and timepart = ? and shard = ? and due > ? and due < now() order by due limit ?")
    @read_shard_last_stmt = @session.prepare("select last from shards where name = ? and shard = ?")
    @update_shard_last_stmt = @session.prepare("update shards set last = ? where name = ? and shard = ?")
    @update_shard_last_now_stmt = @session.prepare("update shards set last = now() where name = ? and shard = ?")

  end

  # Insert a control row for each shard into the shards table.
  # These rows are store the consumer lock and the timestamp of the
  # last processed point in time.
  def init_shards
    for i in 0..@number_of_shards-1 do
      @session.execute(@init_shard_stmt, @name, i)
      @logger.info("Create shard control row for queue #{@name}, shard #{i}")
    end
  end

  # Split a given (GMT) time into the partiction key timepart,
  # the target shard and a time-UUID for the due time
  # The shard is determined using the provided time seconds
  # and calculating the modulo given the number of shards
  def timepart_and_shard_and_due_timeuuid_from_time(gmt_due)
    due_timeuuid = @timeUuidGenerator.from_time(gmt_due)
    shard = gmt_due.sec % @number_of_shards
    timepart = gmt_due.strftime(@timepart_format).to_i
    return timepart,shard,due_timeuuid
  end

  # Enqueue a message with an optional GMT due time
  # gmt_due defaults to GMT now if not specified
  def put_message(message,gmt_due = Time.now.gmtime)
    gmt_due = gmt_due + @due_safety_offset_seconds
    time_part,shard,due_timeuuid = timepart_and_shard_and_due_timeuuid_from_time(gmt_due)
    @logger.debug("Putting message in #{@name}|#{time_part}|#{shard} (due timeuuid: #{due_timeuuid})")
    @session.execute(@put_stmt, @name, time_part,shard, due_timeuuid, message,@message_ttl)
  end

  # Try to get the lock for an unlocked shard
  # Returns the akquired shard number or -1 if all shards
  # are already taken.
  # You can supply an option start shard if you
  # want take_shard to start trying with a particular
  # shard.
  def take_shard(consumer_id,ttl,start_with = 0)

    shard=-1
    i=0
    while i < @number_of_shards  do
      try_shard = i + start_with
      try_shard = try_shard - @number_of_shards if(try_shard >= @number_of_shards)
      @logger.debug("Trying to get lock for queue #{@name}, shard #{try_shard}" )
      @session.execute(@lock_shard_stmt,ttl,consumer_id, @name, try_shard).each do |row|
        if(row['[applied]']) 
          shard = try_shard
        end
      end
      break if(shard >= 0)

     i += 1
    end

    last_due_timeuuid = nil
    @session.execute(@read_shard_last_stmt, @name, shard).each do |row|
      last_due_timeuuid = row['last']
    end

    return shard,last_due_timeuuid

  end


  # Touch a shard lock
  def touch_lock(consumer_id,shard, ttl)
    @session.execute(@touch_lock_stmt,ttl,consumer_id, @name, shard)
    @logger.debug("Touched lock for queue #{@name}, shard #{shard}")
  end

  # Set the last due_timeuuid that has been processed for the
  # given shard
  def set_last_processed(shard,last_processed)
    @session.execute(@update_shard_last_stmt, last_processed,@name,shard)
  end

  def set_last_processed_now(shard)
    @session.execute(@update_shard_last_now_stmt, @name,shard)
  end


  # Read n messages from the queue that are the next
  # due after the specified last-timeuuid. But take only
  # messages with a due-timeuuid less than the CQL now()
  # timeuuid.
  #
  # Returns an array of due messages (struct of due and message),
  # or nil to indicate that now() has been reached and processing
  # should pause a while.
  def take_messages(shard,last_due_timeuuid,n)
    last_gmt = last_due_timeuuid.to_time
    messages = internal_take_messages(shard,last_gmt,last_due_timeuuid,n)
    if(messages == nil)
      return nil
    end
    while messages.length == 0 do
      save_last_gmt = last_gmt
      last_gmt = last_gmt + @shard_granularity_in_seconds
      messages = internal_take_messages(shard,last_gmt,last_due_timeuuid,n)
      if(messages == nil)
        return nil
      end
    end
    return messages
  end


  def internal_take_messages(shard,try_gmt,last_due_timeuuid,n)
    timepart = try_gmt.strftime(@timepart_format).to_i

    x=Time.now
    gm=x.gmtime
    timepart_now = gm.strftime(@timepart_format).to_i
    if(timepart == timepart_now)
      @logger.debug("Current time reached")
      return nil
    end

    @logger.debug("Trying to take #{n} messages from #{@name}, shard #{shard} timepart #{timepart} #{last_due_timeuuid}")
    messages = Array.new
    @session.execute(@take_stmt, @name, timepart,shard,last_due_timeuuid,n).each do |row|
      due,message = row['due'],row['message']
      messages << Message.new(due,message)
    end
    return messages
  end

end

