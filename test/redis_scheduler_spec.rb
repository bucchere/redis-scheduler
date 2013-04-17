require 'redis'
require 'json'
require '/Users/chris/dijit/redis-scheduler/lib/redis-scheduler.rb'

describe RedisScheduler do
  before do
    @redis = Redis.new(:host => 'localhost', :port => 6379)
    @redis.select(9)
    @scheduler = RedisScheduler.new(@redis, :namespace => 'testing')
    @scheduler.reset!
    @scheduler.should_not == nil
    @id1 = @scheduler.schedule!("testing1", Time.now.to_i, 'one')
    @id2 = @scheduler.schedule!("testing2", Time.now.to_i, 'one')
    @id3 = @scheduler.schedule!(URI::encode({ "testing" => 3 }.to_json), Time.now.to_i, 'two')
    @id_future = @scheduler.schedule!("future", (Time.now + 100000).to_i, 'three')
    @scheduler.size.should == 4
  end

  it "should schedule an item" do
    @id = @scheduler.schedule!("testing", Time.now.to_i)
    @scheduler.item(@id).should == { @id => "testing" }
  end

  it "should allow unscheudling by user" do
    @scheduler.unschedule_all_for!('one').should == [{ @id1.to_s => 'testing1' }, { @id2.to_s => 'testing2' }]
    @scheduler.size.should == 2
  end

  it "should return an array of job_id => payload hashes for a given user" do
    @scheduler.scheduled_for('one').should == [{ @id1.to_s => 'testing1' }, { @id2.to_s => 'testing2' }]
    @scheduler.size.should == 4
  end

  it "should unschedule, remove and return a given job for a given user" do
    @scheduler.unschedule!('one', [@id1]).should == [2]
    @scheduler.size.should == 3
  end

  it "should iterate over items ready to be executed" do
    @scheduler.each do |entry, time|
      if entry != 'testing1' and entry != 'testing2'
        JSON::parse(URI::decode(entry))["testing"].should == 3
      end
    end
    @scheduler.size.should == 1
  end
end