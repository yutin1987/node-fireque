var assert = require("assert"),
    yt = require("yutin-node"),
    Fireque = require("../index.js"),
    redis = require("redis"),
    client = redis.createClient(),
    queueName = Fireque._getQueueName();

describe('Work', function(){
  beforeEach(function(done){
    client.flushall(done);
  });
  
  describe('#new Work()', function(){
    var work_dev = new Fireque.Worker();
    it('protocol should return universal', function(){
      assert.equal('universal', work_dev.protocol);
    });
    it('workload should return 100', function(){
      assert.equal(100, work_dev.workload);
    });
    it('wait should return 10', function(){
      assert.equal(10, work_dev._wait);
    });
    it('priority.length should return 6', function(){
      assert.equal(6, work_dev._priority.length);
    });
    it('port should return 6379', function(){
      assert.equal(6379, work_dev._connection.port);
    });
    it('host should return 127.0.0.1', function(){
      assert.equal('127.0.0.1', work_dev._connection.host);
    });
  });

  describe('#Work Perform', function(){
    var job_dev = new Fireque.Job('push', {
        x: 2,
        y: 5
    });
    var work_dev = new Fireque.Worker('push', {
        wait: 1,
        workload: 1
    }, { connection: job_dev._connection});
    
    this.timeout(10000);

    beforeEach(function(done){
      job_dev.enqueue(done);
    });

    it('Completed: job should in completed only 1', function(done){
      var perform = function(job, feedback){
        assert.equal(job_dev.uuid, job.uuid);
        client.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
          assert.equal(work_dev._wrokers.length, 1);
          assert.equal(1, yt.getListCount(reply, job.uuid), 'should in processing only 1');
          feedback(true);
          setTimeout(function(){
            client.lrange(queueName + ':push:completed', -100, 100, function(err, reply){
              assert.equal(1, yt.getListCount(reply, job.uuid));
              work_dev.offPerform(perform);
              assert.equal(work_dev._wrokers.length, 0);
              done();
            });
          }, 100);
        });
      };
      work_dev.onPerform(perform); 
    });

    it('Failed: job should in failed only 1', function(done){
      var perform = function(job, feedback){
        assert.equal(job_dev.uuid, job.uuid);
        client.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
          assert.equal(work_dev._wrokers.length, 1);
          assert.equal(1, yt.getListCount(reply, job.uuid), 'should in processing only 1');
          feedback(false);
          setTimeout(function(){
            client.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
              assert.equal(1, yt.getListCount(reply, job.uuid), 'should in failed only 1');
              work_dev.offPerform(perform);
              assert.equal(work_dev._wrokers.length, 0);
              done();
            });
          }, 100);
        });
      };
      work_dev.onPerform(perform); 
    });

    it('Throw: job should in failed only 1', function(done){
        var perform = function(job, feedback){
        assert.equal(job_dev.uuid, job.uuid);
        client.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
          assert.equal(work_dev._wrokers.length, 1);
          assert.equal(1, yt.getListCount(reply, job.uuid), 'should in processing only 1');
          setTimeout(function(){
            client.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
              assert.equal(1, yt.getListCount(reply, job.uuid), 'should in failed only 1');
              work_dev.offPerform(perform);
              assert.equal(work_dev._wrokers.length, 0);
              done();
            });
          }, 100);
        });
        throw (new Error('throw error test.'));
      };
      work_dev.onPerform(perform); 
    });
  });
});