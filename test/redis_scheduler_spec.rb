require 'redis'
require '/Users/chris/dijit/redis-scheduler/lib/redis-scheduler.rb'

describe RedisScheduler do
  before do
    @redis = Redis.new(:host => 'localhost', :port => 6379)
    @redis.select(9)
    @scheduler = RedisScheduler.new(@redis, :namespace => 'testing')
    @scheduler.reset!
  end

  it "should create a redis scheduler" do
    @scheduler.should_not == nil
  end

  it "should schedule an item" do
    @scheduler.schedule!("testing", Time.now.to_i)
    @scheduler.size.should == 1
    @scheduler.each do |item|
      item.should == "testing"
    end
    @scheduler.size.should == 0
  end

  it "should allow unscheudling by user" do
    @scheduler.schedule!("testing1", Time.now.to_i, 1)
    @scheduler.schedule!("testing2", Time.now.to_i, 1)
    @scheduler.size.should == 2
    @scheduler.unschedule_for!(1).should == ['testing1', 'testing2']
    @scheduler.size.should == 0
  end

  it "should not do too much unscheduling" do
    @scheduler.schedule!("testing1", Time.now.to_i, 1)
    @scheduler.schedule!("testing2", Time.now.to_i, 2)
    @scheduler.size.should == 2
    @scheduler.unschedule_for!(1).should == ['testing1']
    @scheduler.size.should == 1
  end

  it "should return an array of job_id => payload hashes for a given user" do
    id1 = @scheduler.schedule!("testing1", Time.now.to_i, 1)
    id2 = @scheduler.schedule!("testing2", Time.now.to_i, 1)
    id3 = @scheduler.schedule!("testing3", Time.now.to_i, 2)
    @scheduler.size.should == 3
    @scheduler.jobs_for(1).should == [{ id1.to_s => 'testing1' }, { id2.to_s => 'testing2' }]
    @scheduler.size.should == 3
  end
end