require 'redis'
require '/Users/chris/dijit/redis-scheduler/lib/redis-scheduler.rb'

describe RedisScheduler do
  before do
    @redis = Redis.new(:host => 'localhost', :port => 6379)
    @redis.select(9)
    @scheduler = RedisScheduler.new(@redis, :namespace => 'testing')
    @scheduler.reset!
    @scheduler.should_not == nil
    @id1 = @scheduler.schedule!("testing1", Time.now.to_i, 1)
    @id2 = @scheduler.schedule!("testing2", Time.now.to_i, 1)
    @id3 = @scheduler.schedule!("testing3", Time.now.to_i, 2)
    @scheduler.size.should == 3
  end

  it "should schedule an item" do
    @id = @scheduler.schedule!("testing", Time.now.to_i)
    @scheduler.item(@id).should == { @id => "testing" }
  end

  it "should allow unscheudling by user" do
    @scheduler.unschedule_for!(1).should == [{ @id1.to_s => 'testing1' }, { @id2.to_s => 'testing2' }]
    @scheduler.size.should == 1
  end

  it "should return an array of job_id => payload hashes for a given user" do
    @scheduler.scheduled_for(1).should == [{ @id1.to_s => 'testing1' }, { @id2.to_s => 'testing2' }]
    @scheduler.size.should == 3
  end

  it "should unschedule, remove and return a given job for a given user" do
    @scheduler.unschedule!(1, @id1).should == { @id1.to_s => 'testing1' }
    @scheduler.size.should == 2
  end
end