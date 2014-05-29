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
    @job_types = [@namespace, "job_types"].join #hash mapping type (e.g. email_id) => job_id1,job_id2,job_id3....
  end

  ## Schedule an item at a specific time. item will be converted to a string.
  def schedule!(item, time, user_id = nil, job_id = nil, type = nil)
    id = job_id ? job_id : @redis.incr(@counter)
    payload = user_id ? "#{id}:#{user_id}" : "#{id}:-1"
    payload = type ? "#{payload}:#{type}" : payload
    @redis.zadd @queue, time.to_f, payload
    add_job_for(user_id, id, item, type)
    id
  end

  ## Drop all data and reset the schedule entirely.
  def reset!
    [@queue, @processing_set, @counter, @jobs, @user_jobs, @job_types].each { |k| @redis.del k }
  end

  ## Return the total number of items in the schedule.
  def size
    @redis.zcard @queue
  end
  
  ## Return the number of jobs matching the given type in the schedule 
  def size_by(type)
    @redis.hlen(@job_types, type)
  end

  ## Return the number of users who currenly have jobs in the schedule 
  def num_users
    @redis.hlen(@user_jobs)
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
      #ids, at, processing_descriptor, item = x
      #job_id, user_id, type = ids.split(':')
      ids = x[0]
      at = x[1]
      processing_descriptor = x[2]
      item = x[3]
      ids_split = ids[0].split(':')
      job_id = ids_split[0]
      user_id = ids_split[1]
      type = ids_split[2]
      begin
	      yield item, at, job_id
      rescue Exception # back in the hole!
	      schedule! item, at, user_id, job_id, type
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
  def unschedule_all_for!(user_id, type = nil)
    unschedule!(user_id, job_ids_for(user_id, type), type)
  end

  def scheduled_for(user_id, type = nil)
    rval = []
    return rval unless user_id
    job_ids_for(user_id, type).each do |job_id|
      rval << { job_id => @redis.hget(@jobs, job_id) }
    end
    rval
  end

  #O(n) where n is the number of jobs for a given user
  def unschedule!(user_id = nil, job_ids = -1, type = nil)
    raise "user_id is required" unless user_id
    raise "type is required if user_id == -1" if user_id == -1 && type.nil?
    
    job_types_to_delete = {} #will hold type => [job_id1:user_id1, job_id2:user_id2, ...]
    user_jobs_to_delete = {} #create a hash that will hold user_id => [job_id1:type1, job_id2:type2, ...]
    
    @redis.watch @queue, @jobs, @user_jobs, @job_types

    if user_id != -1 #delete all jobs in job_ids (of a type if specified); otherwise delete all the user's jobs of matching type
      user_jobs = @redis.hget(@user_jobs, user_id)
      user_jobs = user_jobs ? JSON::parse(URI::decode(user_jobs)) : []
    
      #iterate over all this user's jobs, removing those in job_ids (and, optionally, matching type)
      user_jobs = user_jobs.map do |uj|
        job_id = uj.to_s.index(':') ? uj.split(':')[0] : uj
        begin
          job_type = uj.split(':')[1]
        rescue
          job_type =  -1
        end
        if job_ids == -1 || (job_ids.is_a?(Array) && job_ids.include?(job_id))
          if job_types_to_delete.include?(job_type)
            if type
              job_types_to_delete[job_type] << "#{job_id}:#{user_id}" if type == job_type
            else
              job_types_to_delete[job_type] << "#{job_id}:#{user_id}"
            end
          else
            if type
              job_types_to_delete[job_type] = ["#{job_id}:#{user_id}"] if type == job_type
            else
              job_types_to_delete[job_type] = ["#{job_id}:#{user_id}"]
            end  
          end
          nil #signals that this item will need to me removed
        else
          "#{job_id}:#{job_type}" #this items stays
        end
      end
      user_jobs = user_jobs.delete_if { |uj| uj.nil? }
      
      job_types_to_delete.each do |job_type, job_and_user_ids|
        job_and_user_ids.each do |juid|
          j_id = juid.split(':')[0]
          u_id = juid.split(':')[1]
          @redis.multi do                
            @redis.zrem(@queue, job_type == -1 ? "#{j_id}:#{u_id}" : "#{j_id}:#{u_id}:#{job_type}")            
            @redis.hdel(@jobs, j_id)            
          end
        end
        job_types = @redis.hget(@job_types, job_type)
        job_types = job_types ? JSON::parse(URI::decode(job_types)) : []
        job_types -= job_and_user_ids
        
        @redis.multi do                
          if job_types.size == 0
            @redis.hdel(@job_types, job_type)
          else
            @redis.hset(@job_types, job_type, URI::encode(job_types.to_json))                      
          end
        end
      end          
      @redis.multi do                
        if user_jobs.size == 0
          @redis.hdel(@user_jobs, user_id)
        else
          @redis.hset(@user_jobs, user_id, URI::encode(user_jobs.to_json))
        end
      end
    else #delete all jobs of a specified type, irrespective of user_id
      job_types = @redis.hget(@job_types, type)
      job_types = job_types ? JSON::parse(URI::decode(job_types)) : []
        
      #iterate over all the jobs of this type, removing those in job_ids (and, optionally, matching user_id)
      if job_ids == -1 || (job_ids.is_a?(Array) && job_ids.size > 0)
        job_types = job_types.map do |jt|
          job_id = jt.split(':')[0]
          u_id = jt.split(':')[1]
          if job_ids == -1 || (job_ids.is_a?(Array) && job_ids.include?(job_id))
            if user_jobs_to_delete.include?(u_id)
              if user_id != -1
                user_jobs_to_delete[u_id] << "#{job_id}:#{type}" if u_id == user_id
              else
                user_jobs_to_delete[u_id] << "#{job_id}:#{type}"
              end
            else
              if user_id != -1
                user_jobs_to_delete[u_id] = ["#{job_id}:#{type}"] if u_id == user_id
              else
                user_jobs_to_delete[u_id] = ["#{job_id}:#{type}"]
              end  
            end
            nil #signals that this item will need to me removed
          else
            "#{job_id}:#{u_id}" #this items stays
          end
        end
        job_types = job_types.delete_if { |jt| jt.nil? }
      end
      
      user_jobs_to_delete.each do |user_id, job_ids_and_types|
        job_ids_and_types.each do |jiat|
          j_id = jiat.split(':')[0]
          t = jiat.split(':')[1]
          @redis.multi do                
            @redis.zrem(@queue, t && t != -1 ? "#{j_id}:#{user_id}:#{t}" : "#{j_id}:#{user_id}")            
            @redis.hdel(@jobs, j_id)            
          end
        end
        user_jobs = @redis.hget(@user_jobs, user_id)
        user_jobs = user_jobs ? JSON::parse(URI::decode(user_jobs)) : []
        user_jobs -= job_ids_and_types
        
        @redis.multi do                
          if user_jobs.size == 0
            @redis.hdel(@user_jobs, user_id)
          else
            @redis.hset(@user_jobs, user_id, URI::encode(user_jobs.to_json))                      
          end
        end
      end
      @redis.multi do                          
        if job_types.size == 0
          @redis.hdel(@job_types, type)
        else
          @redis.hset(@job_types, type, URI::encode(job_types.to_json))
        end
      end
    end
    job_ids
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
      @redis.watch @queue, @jobs, @user_jobs, @job_types
      #ids, runtime = @redis.zrangebyscore(@queue, 0, Time.now.to_f, :withscores => true, :limit => [0, 1])
      ids_runtime = @redis.zrangebyscore(@queue, 0, Time.now.to_f, :withscores => true, :limit => [0, 1])
      ids = ids_runtime.first
      runtime = ids_runtime.last
      break unless ids
      #job_id, user_id, type = ids.split(':')
      job_id_user_id_type = ids[0].split(':')
      job_id = job_id_user_id_type[0]
      user_id = job_id_user_id_type[1]
      type = job_id_user_id_type[2]
      descriptor = Marshal.dump [ids, Time.now.to_f, descriptor]
      payload = @redis.hget(@jobs, job_id.to_s)
      job_ids = type ? ["#{job_id}:#{type}"] : ["#{job_id}"]
      user_jobs = @redis.hget(@user_jobs, user_id)
      user_jobs = user_jobs ? JSON::parse(URI::decode(user_jobs)) : []
      user_jobs -= job_ids  
      job_types = @redis.hget(@job_types, type)
      job_types = job_types ? JSON::parse(URI::decode(job_types)) : []
      job_types -= ["#{job_id}:#{user_id}"]        
      @redis.multi do
        @redis.zrem(@queue, type ? "#{job_id}:#{user_id}:#{type}" : "#{job_id}:#{user_id}")            
        @redis.hdel(@jobs, job_id)            
        if job_types.size == 0
          @redis.hdel(@job_types, type)
        else
          @redis.hset(@job_types, type, URI::encode(job_types.to_json))                      
        end
        if user_jobs.size == 0
          @redis.hdel(@user_jobs, user_id)
        else
          @redis.hset(@user_jobs, user_id, URI::encode(user_jobs.to_json))
        end
      end and break [ids, Time.now.to_f, descriptor, payload]
      sleep CAS_DELAY # transaction failed. retry!
    end
  end

  def cleanup!(item)
    @redis.srem @processing_set, item
  end

  def job_ids_for(user_id, type = nil)
    raise "user_id is required" unless user_id
    raise "type is required if user_id == -1" if user_id == -1 && type.nil?

    jobs_by_user, jobs_by_type = []
    if type
      jobs_by_type = @redis.hget(@job_types, type)
      jobs_by_type = jobs_by_type ? JSON::parse(URI::decode(jobs_by_type)) : []
      jobs_by_type = jobs_by_type.map { |jbt| jbt.to_s.index(':') ? jbt.split(':')[0] : jbt }
    end
    if user_id != -1
      jobs_by_user = @redis.hget(@user_jobs, user_id)
      jobs_by_user = jobs_by_user ? JSON::parse(URI::decode(jobs_by_user)) : []      
      jobs_by_user = jobs_by_user.map { |jbu| jbu.to_s.index(':') ? jbu.split(':')[0] : jbu }
      if type
        jobs_by_user & jobs_by_type
      else
        jobs_by_user
      end
    else
      jobs_by_type
    end
  end

  def add_job_for(user_id, job_id, item, type = nil)
    return unless job_id
    @redis.watch @jobs, @user_jobs, @job_types
    user_jobs = []
    if user_id
      user_jobs = @redis.hget(@user_jobs, user_id)
      user_jobs = user_jobs ? JSON::parse(URI::decode(user_jobs)) : []
      user_jobs << (type ? "#{job_id}:#{type}" : job_id)
    end
    job_types = []
    if type
      job_types = @redis.hget(@job_types, type)
      job_types = job_types ? JSON::parse(URI::decode(job_types)) : []
      job_types << "#{job_id}:#{user_id}"
    end
    @redis.multi do
      @redis.hset(@jobs, job_id, item)
      @redis.hset(@user_jobs, user_id, URI::encode(user_jobs.to_json))
      @redis.hset(@job_types, type, URI::encode(job_types.to_json)) if type
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
