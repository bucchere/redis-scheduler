require 'redis'
require 'json'
require 'timecop'
require 'awesome_print'
require './lib/redis-scheduler.rb'

describe RedisScheduler do
  before do
    @now = Time.now
    @redis = Redis.new(:host => 'localhost', :port => 6379)
    @redis.select(9)
    @scheduler = RedisScheduler.new(@redis, :namespace => 'testing')
    @scheduler.reset!
    #@scheduler.should_not == nil
    @id1 = @scheduler.schedule!('testing1', @now, 1)
    @id2 = @scheduler.schedule!('testing2', @now, 1)
    @id3 = @scheduler.schedule!('testing3', @now, 2)
    @id_future = @scheduler.schedule!('future', @now + 100000, 3)
    #@scheduler.size.should == 4
    @id = nil
  end

  it 'should schedule an item' do
    @id = @scheduler.schedule!('testing', @now, 3)
    @scheduler.item(@id).should == { @id => 'testing' }
    @scheduler.size.should == 5
  end

  it 'should unschedule an item' do
    @id = @scheduler.schedule!('testing', @now.to_f, 3)
    @scheduler.item(@id).should == { @id => 'testing' }
    @scheduler.size.should == 5
    @scheduler.unschedule!(3, [@id]).should == [@id]
    @scheduler.size.should == 4
  end
  
  it 'should allow unscheduling by user' do
    job_ids = @scheduler.unschedule_all_for!(1)
    job_ids[0].should == @id1
    job_ids[1].should == @id2
    @scheduler.size.should == 2 #only two jobs remain on the schedule
    @scheduler.scheduled_for(1).should == [] #no jobs for user 1
  end

  it 'should return an array of job_id => payload hashes for a given user' do
    user1_jobs = @scheduler.scheduled_for(1)
    user1_jobs[0][@id1][0] = 'testing1' 
    user1_jobs[0][@id1][0] = 'testing2' 
    @scheduler.size.should == 4 #no changes to schedule
  end

  it 'should unschedule, remove and return a given job for a given user' do
    @scheduler.unschedule!(1, [@id1]).should == [@id1]
    @scheduler.size.should == 3
  end

  it 'should iterate over items ready to be executed' do
    begin
      Timecop.travel Time.now + 5000
      @scheduler.each do |entry, time, job_id|
        #this space intentionally left blank
      end
      @scheduler.size.should == 1
    ensure 
      Timecop.return
    end
  end
  
  it 'should allow scheulding and unscheduling by type' do
    @id4 = @scheduler.schedule!('testing4', @now, 4, nil, 10)
    @id5 = @scheduler.schedule!('testing5', @now, 5, nil, 10)    
    @scheduler.size.should == 6
    @scheduler.unschedule_all_for!(-1, 11).should == []
    @scheduler.unschedule_all_for!(4, 11).should == []
    @scheduler.size.should == 6
    @scheduler.unschedule_all_for!(4, 10).should == [@id4]
    @scheduler.size.should == 5
    @scheduler.unschedule_all_for!(5, 10).should == [@id5]
    @scheduler.size.should == 4
  end
end