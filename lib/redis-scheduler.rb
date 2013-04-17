require 'logger'
## A basic chronological scheduler for Redis.
##
## Use #schedule! to add an item to be processed at an arbitrary point in time.
## The item will be converted to a string and later returned to you as such.
##
## Use #each to iterate over those items in the schedule which are ready for
## processing. In blocking mode, this call will never terminate. In nonblocking
## mode, this call will terminate when there are no items ready for processing.
##
## Use #items to iterate over all items in the queue, for debugging purposes.
##
## == Ensuring reliable behavior in the presence of segfaults
##
## The scheduler maintains a "processing set" of items currently being
## processed. If a process dies (i.e. not as a result of a Ruby exception, but
## as the result of a segfault), the item will remain in this set but will
## not longer appear in the schedule. To avoid losing scheduled work due to
## segfaults, you must periodically iterate through this set and recover
## any items that have been abandoned, using #processing_set_items. Setting a
## proper 'descriptor' argument in #each is suggested.
class RedisScheduler
  include Enumerable

  POLL_DELAY = 1.0 # seconds
  CAS_DELAY  = 0.5 # seconds

  ## Options:
  ## * +namespace+: prefix for Redis keys, e.g. "scheduler/"
  ## * +blocking+: whether #each should block or return immediately if there are items to be processed immediately.
  ##
  ## Note that nonblocking mode may still actually block momentarily as part of
  ## the check-and-set semantics, i.e. block during contention from multiple
  ## clients. "Nonblocking" refers to whether the scheduler should wait until
  ## events in the schedule are ready, or only return those items that are
  ## ready currently.
  def initialize redis, opts={}
    @redis = redis
    @namespace = opts[:namespace]
    @blocking = opts[:blocking]
    @logger = opts[:logger] || Logger.new(STDOUT)
    @queue = [@namespace, "q"].join
    @processing_set = [@namespace, "processing"].join
    @counter = [@namespace, "counter"].join
    @jobs = [@namespace, "jobs"].join #hash mapping job_id => payload
    @user_jobs = [@namespace, "user_jobs"].join #hash mapping user_id => job_id1,job_id2,job_id3....
  end

  ## Schedule an item at a specific time. item will be converted to a string.
  def schedule!(item, time, user_id = nil)
    id = @redis.incr @counter
    @redis.zadd @queue, time.to_f, user_id ? "#{id}:#{user_id}" : "#{id}"
    add_job_for(user_id, id, item)
    id
  end

  ## Drop all data and reset the schedule entirely.
  def reset!
    [@queue, @processing_set, @counter, @jobs, @user_jobs].each { |k| @redis.del k }
  end

  ## Return the total number of items in the schedule.
  def size
    @redis.zcard @queue
  end

  ## Returns the total number of items currently being processed.
  def processing_set_size
    @redis.scard @processing_set
  end

  ## Yields items along with their scheduled times. only returns items on or
  ## after their scheduled times. items are returned as strings. if @blocking is
  ## false, will stop once there are no more items that can be processed
  ## immediately; if it's true, will wait until items become available (and
  ## never terminate).
  ##
  ## +Descriptor+ is an optional string that will be associated with this item
  ## while in the processing set. This is useful for providing whatever
  ## information you need to determine whether the item needs to be recovered
  ## when iterating through the processing set.
  def each descriptor=nil
    while(x = get(descriptor))
      ids, at, processing_descriptor, item = x
      job_id, user_id = ids.split(':')
      begin
	yield item, at
      rescue Exception # back in the hole!
	schedule! item, at, user_id
	raise
      ensure
	cleanup! processing_descriptor
      end
    end
  end

  ## Returns an Enumerable of [item, scheduled time] pairs, which can be used
  ## to iterate over all the items in the queue, in order of earliest- to
  ## latest-scheduled, regardless of the schedule time.
  ##
  ## Note that this view is not synchronized with write operations, and thus
  ## may be inconsistent (e.g. return duplicates, miss items, etc) if changes
  ## to the schedule happen while iterating.
  ##
  ## For these reasons, this is mainly useful for debugging purposes.
  def items
    ItemEnumerator.new(@redis, @queue)
  end

  ## Returns an Array of [item, timestamp, descriptor] tuples representing the
  ## set of in-process items. The timestamp corresponds to the time at which
  ## the item was removed from the schedule for processing.
  def processing_set_items
    @redis.smembers(@processing_set).map do |x|
      ids, timestamp, descriptor = Marshal.load(x)
      [ids, Time.at(timestamp), descriptor]
    end
  end

  #unschedules all jobs for a given user
  def unschedule_all_for!(user_id)
    rval = []
    return rval unless user_id
    #TODO consider using hmget instead of looping
    jobs_ids_for(user_id).each do |job_id|
      rval << { job_id => @redis.hget(@jobs, job_id) }
      @redis.zrem(@queue, "#{job_id}:#{user_id}")
      @redis.hdel(@jobs, job_id)
    end
    @redis.hdel(@user_jobs, user_id)
    rval
  end

  def scheduled_for(user_id)
    rval = []
    return rval unless user_id
    #TODO consider using hmget instead of looping
    jobs_ids_for(user_id).each do |job_id|
      rval << { job_id => @redis.hget(@jobs, job_id) }
    end
    rval
  end

  #O(n) where n is the number of jobs for a given user
  def unschedule!(user_id, job_ids)
    raise unless user_id and job_ids and job_ids.class == Array
    @redis.zrem(@queue, job_ids.map { |job_id| "#{job_id}:#{user_id}" })
    @redis.hdel(@jobs, job_ids)
    jobs_raw = @redis.hget(@user_jobs, user_id)
    jobs = jobs_raw ? JSON::parse(URI::decode(jobs_raw)) : []
    jobs -= job_ids
    if jobs.size == 0
      @redis.hdel(@user_jobs, user_id)
    else
      @redis.hset(@user_jobs, user_id, URI::encode(jobs.to_json))
    end
    jobs
  end

  def item(job_id)
    return nil unless job_id
    { job_id => @redis.hget(@jobs, job_id) }
  end

  private

  def get(descriptor)
    @blocking ? blocking_get(descriptor) : nonblocking_get(descriptor)
  end

  def blocking_get descriptor
    sleep POLL_DELAY until(x = nonblocking_get(descriptor))
    x
  end

  ## Thrown by some RedisScheduler operations if the item in Redis zset
  ## underlying the schedule is not parseable. This should basically never
  ## happen, unless you are naughty and are adding/removing items from that
  ## zset yourself.
  class InvalidEntryException < StandardError; end

  def nonblocking_get descriptor
    loop do
      @redis.watch @queue
      ids_and_time = @redis.zrangebyscore(@queue, 0, Time.now.to_f, :withscores => true, :limit => [0, 1])[0]
      break unless ids_and_time
      job_id, user_id = ids_and_time[0].split(':')
      runtime = ids_and_time[1]
      descriptor = Marshal.dump [user_id ? "#{job_id}:#{user_id}" : job_id, Time.now.to_f, descriptor]

      if user_id
	jobs_raw = @redis.hget(@user_jobs, user_id)
	jobs = jobs_raw ? JSON::parse(URI::decode(jobs_raw)) : []
	jobs.each do |job|
	  if job == job_id
	    jobs -= [job]
	    break
	  end
	end
	if jobs.size == 0
	  @redis.hdel(@user_jobs, user_id)
	else
	  @redis.hset(@user_jobs, user_id, URI::encode(jobs.to_json))
	end
      end
      payload = @redis.hget(@jobs, job_id)

      @redis.multi do # try and grab it
	@redis.zrem @queue, ids_and_time[0]
	@redis.sadd @processing_set, descriptor
	@redis.hdel(@jobs, job_id)
      end and break [user_id ? "#{job_id}:#{user_id}" : job_id, Time.now.to_f, descriptor, payload]
      sleep CAS_DELAY # transaction failed. retry!
    end
  end

  def cleanup!(item)
    @redis.srem @processing_set, item
  end

  def jobs_ids_for(user_id)
    return [] unless user_id
    jobs = @redis.hget(@user_jobs, user_id)
    jobs ? JSON::parse(URI::decode(jobs)) : []
  end

  def add_job_for(user_id, job_id, item)
    return unless job_id
    @redis.hset(@jobs, job_id, item)
    if user_id
      jobs_raw = @redis.hget(@user_jobs, user_id)
      jobs = jobs_raw ? JSON::parse(URI::decode(jobs_raw)) : []
      jobs << job_id
      @redis.hset(@user_jobs, user_id, URI::encode(jobs.to_json))
    end
  end

  ## Enumerable class for iterating over everything in the schedule. Paginates
  ## calls to Redis under the hood (and is thus usable for very large
  ## schedules), but is not synchronized with write traffic and thus may return
  ## duplicates or skip items when paginating.
  ##
  ## Supports random access with #[], with the same caveats as above.
  class ItemEnumerator
    include Enumerable
    def initialize redis, q
      @redis = redis
      @q = q
    end

    PAGE_SIZE = 50
    def each
      start = 0
      while start < size
        elements = self[start, PAGE_SIZE]
        elements.each { |*x| yield(*x) }
        start += elements.size
      end
    end

    def [] start, num=nil
      elements = @redis.zrange @q, start, start + (num || 0) - 1, :withscores => true
      v = elements.each_slice(2).map do |job_id, at|
        [@redis.hget(@jobs, job_id), Time.at(at.to_f)]
      end
      num ? v : v.first
    end

    def size; @redis.zcard @q end
  end
end
