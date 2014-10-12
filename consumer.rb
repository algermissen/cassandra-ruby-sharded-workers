#!/usr/bin/env ruby
require 'optparse'
require_relative 'sharded_queue'

# Start a thread to periodically touch our shard lock
def start_lock_touch_thread(logger,queue,worker_id,shard,ttl)
  Thread.new do
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
end

# Place holder for message 'processing'
def process_message(logger,m)
    t = m.due.to_time
    logger.info("MESSAGE: #{t} #{m.message}")
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

start_lock_touch_thread(logger,queue,worker_id,shard,lock_ttl)

loop do
  #puts "loop #{shard}"
  messages = queue.take_messages(shard,last_due_processed,number_to_take)
  if(messages == nil)
    logger.info("No more messages pending until current time, exiting")
    queue.set_last_processed_now(shard)
    exit 0
  end
  
  messages.each do |m| 
    process_message(logger,m)
  end
  queue.set_last_processed(shard,messages.last.due)
  last_due_processed = messages.last.due

  sleep 1
end









