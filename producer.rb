#!/usr/bin/env ruby
require 'optparse'
require_relative 'sharded_queue'

host     = '127.0.0.1'
keyspace = 'foo'
name     = 'test'

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: producer.rb [options]"

  opts.on("-H", "--host HOST", "Cassandra host") { |h| host = h }
  opts.on("-K", "--keyspace KEYSPACE", "Keyspace to use") { |k| keyspace = k }
  opts.on("-N", "--name NAME", "Specifies the queue") { |n| name = n }
end.parse!

queue = ShardedQueue.new(name,host,keyspace)

i=0
loop do
  i += 1
  message="Message #{i} from producer #{$$}"
  queue.put_message(message)

  # You can alternatively specify any time in the future,
  #for example, in one hour
  # queue.put_message(message,Time.now.gmtime + 3600)

  sleep 1
end

