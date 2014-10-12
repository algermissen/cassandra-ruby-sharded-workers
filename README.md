cassandra-ruby-sharded-workers
==============================

This project provides a possible implementation of a time based priority
queue with cassandra. The implementation is suitable for the use cases
that have the following properties:

- There are one or more producers that can work in parallel
- The messages put into the queue for processing are independent, the order in which they are processed is insignificant
- The messages are idempotent, processing them several times does not change the processing result
- There are no strict timing expectations on the processing beyond that messages should ne processed some (reasonably short) time after being due

# Installation

Install the Datastax Cassandra driver for Ruby](https://github.com/datastax/ruby-driver). Currently this works best
by downloading the source from github and installing the gem from these sources directly.

Create a keyspace and initialize the worker shards for a queue.

    $ ./init_shards.rb --host <ip> --keyspace <keyspace> --name test

# Produce Some Messages

Fire up a one or a couple of producers to put some messages into the queue:

    $ ./producer.rb --host <ip> --keyspace <keyspace> --name test

# Consume the Messages

For all shards to be processed you need to start at least as many consumers as shards exist.
The number of shards is hard coded in the ShardedQueue constructor:
      
      @number_of_shards = 6

You can start six consumers for example like this:

    $ for i in {1..6}; do ./consumer.rb --host <ip> --keyspace <keyspace> --name test > ./log_$i 2>&1 & sleep 1; done

The sleep prevents the consumers to overwhelm the shard locks when starting up.

# The Details

The design aims to work around the tombstones problem of Cassandra queueing use cases
(see [Cassandra anti-patterns: Queues and queue-like datasets](http://www.datastax.com/dev/blog/cassandra-anti-patterns-queues-and-queue-like-datasets))
by avoiding deletes altogether. 

A queue is realized as a collection of rows using a fraction of the due time as
part of the row key. That way, messages are distributed across rows that become
historic as time and processing advances. The deletion problem is solved by
letting these rows expire some (longer) time beyond the time they must have
been processed anyway (that is, when noone is interested in the messages anymore
anyway)

In order to achieve further distribution of messages and thus allow parallel
processing a modulo-based shard is added to the row key. This shard is calculated
based on the clock seconds of the due time:

    shard = due_time.sec % number_of_shards

This is the working table, containing all the shards:

    create table if not exists work (
      name text,
      timepart bigint,
      shard bigint,
      due timeuuid,
      message text,
      primary key ((name,timepart,shard),due)
     )
     with clustering order by (due asc)

Note the compound row key (name,timepart,shard) which indicates the message distribution
accross the cluster, preventing hot spots.

## Message Producers

When you start a producer you can observe this sharding in the produced log messages. For example,
you see below that messages are inserted in the 'test' queue, time-part '20142851917' and you see a
revolving shard number. The time-part format is year:2014 day:285 hour:19 minute:17. We are using
a minute granularity timepart here, but this could be days or hours, as well - look at the ShardedQueue
constructor source code).

    ./producer.rb -H ... -K ... -N test
    I, [2014-10-12T...]  INFO -- : Cassandra connection established and got session
    D, [2014-10-12T...] DEBUG -- : Putting message in test|20142851917|2 (due timeuuid: 6a419a9c-5244-11e4-b7f3-1bdb6943b387)
    D, [2014-10-12T...] DEBUG -- : Putting message in test|20142851917|3 (due timeuuid: 6ae3a012-5244-11e4-b7f3-1bdb6943b387)
    D, [2014-10-12T...] DEBUG -- : Putting message in test|20142851917|4 (due timeuuid: 6b911c74-5244-11e4-b7f3-1bdb6943b387)
    D, [2014-10-12T...] DEBUG -- : Putting message in test|20142851917|5 (due timeuuid: 6c37ddfc-5244-11e4-b7f3-1bdb6943b387)
    D, [2014-10-12T...] DEBUG -- : Putting message in test|20142851917|0 (due timeuuid: 6cd351d8-5244-11e4-b7f3-1bdb6943b387)
    D, [2014-10-12T...] DEBUG -- : Putting message in test|20142851917|1 (due timeuuid: 6d727948-5244-11e4-b7f3-1bdb6943b387) 

The shards contain messages sorted by due time in ascending order. To eliminate the possibility
of two messages having the same timestamp, due times are represented as time-UUId values rather than
timestamps.

Parallel consumers need not be coordinated because Cassandra will take care of the soring for us.

Because consumers will process the shards up to the current time a delay (60 seconds) is added to the due date
when a message is put into the queue. This delay prevents consumers from missing messages that have a due date
close to 'now' in case there is an overlap between producing and consuming into/from a given shard.

## Message Consumers

Consumers process shards in isolation. There is a control table that stores information about the
shards, among other things a per-shard lock. Upon startup, consumers try to akquire the lock
of a shard (Using C* 2.x conditional updates) and will stick to that shard until they explicitly
release the lock or crash.

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

In order to prevent 'dangling' locks when a consumer crashes, locks are written with a TTL and
consumers are required to periodically touch the lock while they are operating.

The isolation reduces lock contention that might frequently occur when concurrent
consumers would try to akquire a shared lock every time they fetch some messages.

The 'last' column stores for every shard the time-UUID that has last been processed.
This indicates the point in time at which a consumer must pick up work
when it continues to process the queue.

Consumers will process a shard from the 'last' timestamp up to the current time.

After having processed a bunch of messages a consumer should update the 'last'
timestamp to narrow the window of re-processing messages when an accidentally
crashed consumers come up again.

# Things to Note

The design depends on having at least as many consumers as shards to avoid shards
that will not be processed. A useful extension would be to implement consumers to
periodically check for unprocessed shards (for example by looking for a 'last'
timestamp in the distant past combined with a missing look) and spawn off
additional processing for such a shard.

Special care must be taken that the lock is reliably touched to prevent other
consumers from accidentally grabbing the lock. A useful addition would be
to do periodic checks and triggering an 'emergency stop' for a suspicios shard.


