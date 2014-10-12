#!/usr/bin/env ruby
require 'optparse'
require_relative 'sharded_queue'

# Start a thread to periodically touch our shard lock
def start_lock_touch_thread(logger,queue,worker_id,shard,ttl)
  t = Thread.new do
    loop do
      sleep(ttl - 5)
      begin
        queue.touch_lock(worker_id,shard,ttl)
      rescue StandardError => e
        @logger.error("Unable to touch lock, aborting processing immediately. #{e}")
        exit 1
      end
    end
  end
  return t
end

# Place holder for message 'processing'
def process_message(logger,m)
    t = m.due.to_time
    #logger.info("MESSAGE: #{t} #{m.message}")
    STDERR.write(".")
end

host           = '127.0.0.1'
keyspace       = 'foo'
name           = 'test'
lock_ttl       = 20
number_to_take = 10 

logger = Logger.new(STDERR)
logger.level = Logger::DEBUG

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: consumer.rb [options]"

  opts.on("-H", "--host HOST", "Cassandra host") { |h| host = h }
  opts.on("-K", "--keyspace KEYSPACE", "Keyspace to use") { |k| keyspace = k }
  opts.on("-N", "--name NAME", "Specifies the queue") { |n| name = n }
end.parse!

worker_id = Cassandra::TimeUuid::Generator.new.next.to_s

logger.info("Starting worker #{worker_id}")
queue = ShardedQueue.new(name,host,keyspace)

shard,last_due_processed = queue.take_shard(worker_id,lock_ttl)
if(shard < 0)
  logger.info("All shards taken")
  exit 0
end

logger.info("Working on queue #{name}, shard #{shard} with last_due_processed #{last_due_processed}")

touch_thread = start_lock_touch_thread(logger,queue,worker_id,shard,lock_ttl)

loop do
  messages = queue.take_messages(shard,last_due_processed,number_to_take)
  # If we have reached now() stop processing this shard and try to lock
  # on to the next one. This helps to eventually process all shards
  # even if the number of consumers is smaller than the number of shards
  # (given the consumer capacity is larger than the message arrival
  # frequency of course)
  if(messages == nil)
    logger.info("No more messages pending until current time, sleeping")
    queue.set_last_processed_now(shard)
    # Stop touching the lock of this shard
    touch_thread.exit
    logger.debug("Touch thread has exited")
    sleep 3
    
    shard,last_due_processed = queue.take_shard(worker_id,lock_ttl,shard+1)
    if(shard < 0)
      logger.info("All shards taken, exiting now")
      exit 0
    end
    logger.info("Working on queue #{name}, shard #{shard} with last_due_processed #{last_due_processed}")
    touch_thread = start_lock_touch_thread(logger,queue,worker_id,shard,lock_ttl)
    next
  end
  
  messages.each do |m| 
    process_message(logger,m)
  end
  STDERR.write("\n")
  queue.set_last_processed(shard,messages.last.due)
  last_due_processed = messages.last.due

  sleep 0.1
end









