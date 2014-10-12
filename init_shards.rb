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
queue.init_shards

