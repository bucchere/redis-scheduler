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

  it "should allow unscheudling" do
    @scheduler.schedule!("testing1", Time.now.to_i, 1)
    @scheduler.schedule!("testing2", Time.now.to_i, 1)
    @scheduler.size.should == 2
    @scheduler.unschedule_for!(1)
    @scheduler.size.should == 0
  end
end