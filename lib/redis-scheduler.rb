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
    @logger.debug("f-begin initialize: nil")
    @queue = [@namespace, "q"].join
    @processing_set = [@namespace, "processing"].join
    @counter = [@namespace, "counter"].join
    @jobs = [@namespace, "jobs"].join #hash mapping job_id => payload
    @user_jobs = [@namespace, "user_jobs"].join #hash mapping user_id => job_id1,job_id2,job_id3....
  end

  ## Schedule an item at a specific time. item will be converted to a string.
  def schedule!(item, time, user_id = nil)
    begin
      @logger.debug("f-begin schedule!: #{scheduled_for(1)}")
      id = @redis.incr @counter
      @redis.zadd @queue, time.to_f, user_id ? "#{id}:#{user_id}" : "#{id}"
      @redis.hset @jobs, id.to_s, item.to_s
      add_job_for(user_id, id)
      id
    ensure
      @logger.debug("f-end schedule!: #{scheduled_for(1)}")
    end
  end

  ## Drop all data and reset the schedule entirely.
  def reset!
    begin
      @logger.debug("f-begin reset!: #{scheduled_for(1)}")
      [@queue, @processing_set, @counter, @jobs, @user_jobs].each { |k| @redis.del k }
    ensure
      @logger.debug("f-end reset!: #{scheduled_for(1)}")
    end
  end

  ## Return the total number of items in the schedule.
  def size
    begin
      @logger.debug("f-begin size: #{scheduled_for(1)}")
      @redis.zcard @queue
    ensure
      @logger.debug("f-end size: #{scheduled_for(1)}")
    end
  end

  ## Returns the total number of items currently being processed.
  def processing_set_size
    begin
      @logger.debug("f-begin processing_set_size: #{scheduled_for(1)}")
      @redis.scard @processing_set
    ensure
      @logger.debug("f-end processing_set_size: #{scheduled_for(1)}")
    end
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
    begin
      @logger.debug("f-begin each: #{scheduled_for(1)}")
      while(x = get(descriptor))
	ids, at, processing_descriptor = x
	job_id, user_id = ids.split(':')
	item = @redis.hget @jobs, job_id.to_s
	@redis.hdel @jobs, job_id.to_s
	remove_job_for(user_id, job_id) if user_id
	begin
	  yield item, at
	rescue Exception # back in the hole!
	  schedule! item, at
	  @redis.hset @jobs, job_id.to_s, item.to_s
	  add_job_for(user_id, job_id) if user_id
	  raise
	ensure
	  cleanup! processing_descriptor
	end
      end
    ensure
      @logger.debug("f-end each: #{scheduled_for(1)}")
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
    begin
      @logger.debug("f-begin items: #{scheduled_for(1)}")
      ItemEnumerator.new(@redis, @queue)
    ensure
      @logger.debug("f-end items: #{scheduled_for(1)}")
    end
  end

  ## Returns an Array of [item, timestamp, descriptor] tuples representing the
  ## set of in-process items. The timestamp corresponds to the time at which
  ## the item was removed from the schedule for processing.
  def processing_set_items
    begin
      @logger.debug("f-begin processing_set_items: #{scheduled_for(1)}")
      @redis.smembers(@processing_set).map do |x|
	job_id, timestamp, descriptor = Marshal.load(x)
	[job_id, Time.at(timestamp), descriptor]
      end
    ensure
      @logger.debug("f-end processing_set_items: #{scheduled_for(1)}")
    end
  end

  #unschedules all jobs for a given user
  def unschedule_for!(user_id)
    begin
      @logger.debug("f-begin unschedule_for!: #{scheduled_for(1)}")
      rval = []
      return rval unless user_id
      jobs_for(user_id).each do |job_id|
	rval << { job_id.to_s => @redis.hget(@jobs, job_id.to_s) }
	@redis.zrem(@queue, "#{job_id}:#{user_id}")
      end
      @redis.hdel(@user_jobs, user_id.to_s)
      rval
    ensure
      @logger.debug("f-end unschedule_for!: #{scheduled_for(1)}")
    end
  end

  def scheduled_for(user_id)
    rval = []
    return rval unless user_id
    jobs_for(user_id).each do |job_id|
      next unless job_id
      rval << { job_id.to_s => @redis.hget(@jobs, job_id.to_s) }
    end
    rval
  end

  def unschedule!(user_id, id)
    begin
      @logger.debug("f-begin unschedule!: #{scheduled_for(1)}")
      @logger.debug "in unschedule!"
      @logger.debug "user id = #{user_id}"
      @logger.debug "id = #{id}"
      remaining_job_ids = []
      rval = {}
      return rval unless user_id and id
      jobs_for(user_id).each do |job_id|
	if job_id == id
	  @logger.debug "about to unschedule job id = #{job_id}"
	  rval = { job_id.to_s => @redis.hget(@jobs, job_id) }
	  @logger.debug " nothing removed yet #{@redis.zcount(@queue, 0, 10000000000)}"
	  @redis.zrem(@queue, "#{job_id}:#{user_id}")
	  @logger.debug "should have been removed from queue #{@redis.zcount(@queue, 0, 10000000000)}"
	  @logger.debug "not removed from jobs list yet #{@redis.hgetall(@jobs).size}"
	  remove_job_for(user_id, job_id.to_s)
	  @logger.debug "should have been removed from jobs now #{@redis.hgetall(@jobs).size}"
	  @logger.debug "not removed yet from user job list yet #{scheduled_for(user_id)}"
	  @redis.hdel(@jobs, job_id.to_s)
	  @logger.debug "should have been removed from user job list now #{scheduled_for(user_id)}"
	  break
	end
	@logger.debug "skipping #{job_id} -- doesn't match"
      end
      rval
    ensure
      @logger.debug("f-end unschedule!: #{scheduled_for(1)}")
    end
  end

  def item(job_id)
    begin
      @logger.debug("f-begin item: #{scheduled_for(1)}")
      return nil unless job_id
      { job_id => @redis.hget(@jobs, job_id.to_s) }
    ensure
      @logger.debug("f-end item: #{scheduled_for(1)}")
    end
  end

  private

  def get(descriptor)
    begin
      @logger.debug("f-begin get: #{scheduled_for(1)}")
      @blocking ? blocking_get(descriptor) : nonblocking_get(descriptor)
    ensure
      @logger.debug("f-end get: #{scheduled_for(1)}")
    end
  end

  def blocking_get descriptor
    begin
      @logger.debug("f-begin blocking_get: #{scheduled_for(1)}")
      sleep POLL_DELAY until(x = nonblocking_get(descriptor))
      x
    ensure
      @logger.debug("f-end blocking_get: #{scheduled_for(1)}")
    end
  end

  ## Thrown by some RedisScheduler operations if the item in Redis zset
  ## underlying the schedule is not parseable. This should basically never
  ## happen, unless you are naughty and are adding/removing items from that
  ## zset yourself.
  class InvalidEntryException < StandardError; end

  def nonblocking_get descriptor
    begin
      @logger.debug("f-begin nonblocking_get: #{scheduled_for(1)}")
      loop do
	@redis.watch @queue
	ids_and_time = @redis.zrangebyscore(@queue, 0, Time.now.to_f, :withscores => true, :limit => [0, 1])[0]
	break unless ids_and_time
	job_id, user_id = ids_and_time[0].split(':')
	runtime = ids_and_time[1]
	descriptor = Marshal.dump [job_id, Time.now.to_f, descriptor]

	jobs_raw = @redis.hget(@user_jobs, user_id.to_s)
	jobs = jobs_raw ? JSON::parse(URI::decode(jobs_raw)) : []
	jobs.each do |job|
	  if job == job_id
	    jobs -= [job]
	    break
	  end
	end

	@redis.multi do # try and grab it
	  @redis.zrem @queue, ids_and_time[0]
	  @redis.sadd @processing_set, descriptor
	  @redis.hset(@user_jobs, user_id.to_s, URI::encode(jobs.to_json))
	end and break [job_id, Time.at(runtime.to_f), descriptor]
	sleep CAS_DELAY # transaction failed. retry!
      end
    ensure
      @logger.debug("f-end nonblocking_get: #{scheduled_for(1)}")
    end
  end

  def cleanup!(item)
    begin
      @logger.debug("f-begin cleanup!: #{scheduled_for(1)}")
      @redis.srem @processing_set, item
    ensure
      @logger.debug("f-end cleanup!: #{scheduled_for(1)}")
    end
  end

  def jobs_for(user_id)
    return [] unless user_id
    jobs = @redis.hget(@user_jobs, user_id.to_s)
    jobs ? JSON::parse(URI::decode(jobs)) : []
  end

  def add_job_for(user_id, job_id)
    begin
      @logger.debug("f-begin add_job_for: #{scheduled_for(1)}")
      return unless user_id and job_id
      jobs_raw = @redis.hget(@user_jobs, user_id.to_s)
      jobs = jobs_raw ? JSON::parse(URI::decode(jobs_raw)) : []
      jobs << job_id
      @redis.hset(@user_jobs, user_id.to_s, URI::encode(jobs.to_json))
    ensure
      @logger.debug("f-end add_job_for: #{scheduled_for(1)}")
    end
  end

  #O(n) where n is the number of jobs for a given user
  def remove_job_for(user_id, job_id)
    begin
      @logger.debug("f-begin remove_job_for: #{scheduled_for(1)}")
      @logger.debug "in remove job for"
      return unless user_id and job_id
      @logger.debug "user_id = #{user_id}"
      @logger.debug "job_id = #{job_id}"
      jobs_raw = @redis.hget(@user_jobs, user_id.to_s)
      @logger.debug "before jobs raw = #{jobs_raw.inspect}"
      jobs = jobs_raw ? JSON::parse(URI::decode(jobs_raw)) : []
      @logger.debug "before jobs parsed = #{jobs}"
      jobs.each do |job|
	if job.to_s == job_id.to_s
	  jobs -= [job]
	  @logger.debug "removing job #{job}"
	  break
	end
	@logger.debug "skipping job = #{job}"
      end
      @redis.hset(@user_jobs, user_id.to_s, URI::encode(jobs.to_json))
      @logger.debug "after removal = #{@redis.hget(@user_jobs, user_id.to_s)}"
    ensure
      @logger.debug("f-end remove_job_for: #{scheduled_for(1)}")
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
        [@redis.hget(@jobs, job_id.to_s), Time.at(at.to_f)]
      end
      num ? v : v.first
    end

    def size; @redis.zcard @q end
  end
end
