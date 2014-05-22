require 'redis'
require 'json'
require './lib/redis-scheduler.rb'

describe RedisScheduler do
  before do
    @redis = Redis.new(:host => 'localhost', :port => 6379)
    @redis.select(9)
    @scheduler = RedisScheduler.new(@redis, :namespace => 'testing')
    @scheduler.reset!
    @scheduler.should_not == nil
    @id1 = @scheduler.schedule!("testing1", Time.now.to_i, 'adam')
    @id2 = @scheduler.schedule!("testing2", Time.now.to_i, 'adam')
    @id3 = @scheduler.schedule!(URI::encode({ "testing" => 3 }.to_json), Time.now.to_i, 'bob')
    @id_future = @scheduler.schedule!("future", (Time.now + 100000).to_i, 'carl')
    @scheduler.size.should == 4
  end

  it "should schedule an item" do
    @id = @scheduler.schedule!("testing", Time.now.to_i)
    @scheduler.item(@id).should == { @id => "testing" }
  end

  it "should raise an error if you try to unschedule without specifying either a user or a type" do
    expect { @scheduler.unschedule! }.to raise_error(RuntimeError)
  end

  it "should allow unscheudling by user" do
    @scheduler.unschedule_all_for!('adam').should == [@id1, @id2]
    @scheduler.size.should == 2
    @scheduler.scheduled_for('adam').should == []
  end

  it "should return an array of job_id => payload hashes for a given user" do
    @scheduler.scheduled_for('adam').should == [{ @id1 => 'testing1' }, { @id2  => 'testing2' }]
    @scheduler.size.should == 4
  end

  it "should unschedule, remove and return a given job for a given user" do
    @scheduler.unschedule!('adam', [@id1]).should == [@id1]
    @scheduler.size.should == 3
  end

  it "should iterate over items ready to be executed" do
    @scheduler.each do |entry, time, job_id|
      if entry != 'testing1' and entry != 'testing2'
        JSON::parse(URI::decode(entry))["testing"].should == 3
      end
    end
    @scheduler.size.should == 1
  end
  
  it "should allow unscheduling by user by type" do
    @scheduler.reset!
    @scheduler.size.should == 0
    @id1 = @scheduler.schedule!("testing1", Time.now.to_i, 'adam', nil, 'red')
    @id2 = @scheduler.schedule!("testing2", Time.now.to_i, 'adam', nil, 'red')  
    @id3 = @scheduler.schedule!("testing3", Time.now.to_i, 'bob', nil, 'red')
    @scheduler.size.should == 3
    @scheduler.unschedule_all_for!('adam', 'red').should == [@id1.to_s, @id2.to_s]
    @scheduler.scheduled_for('bob') == @id3
    @scheduler.size.should == 1
  end

  it "should allow unscheduling by type (across different users)" do
    @scheduler.reset!
    @scheduler.size.should == 0
    @id1 = @scheduler.schedule!("testing1", Time.now.to_i, 'adam', nil, 'red')
    @id2 = @scheduler.schedule!("testing2", Time.now.to_i, 'bob', nil, 'red')  
    @id3 = @scheduler.schedule!("testing3", Time.now.to_i, 'adam', nil, 'blue')
    @scheduler.size.should == 3
    @scheduler.unschedule_all_for!(-1, 'red').should == [@id1.to_s, @id2.to_s]
    @scheduler.scheduled_for('adam') == @id3
    @scheduler.size.should == 1
  end
end