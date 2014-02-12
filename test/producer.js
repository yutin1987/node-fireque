var assert = require("assert"),
    yt = require("yutin-node"),
    Fireque = require("../index.js"),
    redis = require("redis"),
    client = redis.createClient(),
    queueName = Fireque._getQueueName();

describe('Producer', function(){
  beforeEach(function(done){
    client.flushall(done);
  });

  describe('#Producer Base', function(){
    var job_dev = new Fireque.Job('push', {
      x: 2,
      y: 5
    });
    var work_dev = new Fireque.Worker('push', {
      wait: 1,
      workload: 1
    });
    var producer_dev = new Fireque.Producer('push', {
      wait: 1
    });

    this.timeout(30000);

    beforeEach(function(done){
      job_dev.enqueue(done);
    });

    it('should get completed job form producer', function(done){
      work_dev.onPerform(function(job, feedback){
        assert.equal(job_dev.uuid, job.uuid);
        feedback(true);
        work_dev.offPerform();
      });
      var process = function(job){
        assert.equal(producer_dev._event_completed.length, 1);
        assert.equal(job_dev.uuid, job[0].uuid);
        setTimeout(function(){
          producer_dev.offCompleted(process);
          assert.equal(producer_dev._event_completed.length, 0);
          done();
        });
      };
      producer_dev.onCompleted(process, 1);
    });

    it('should get failed job form producer', function(done){
      work_dev.onPerform(function(job, feedback){
        assert.equal(job_dev.uuid, job.uuid);
        feedback(false);
        work_dev.offPerform();
      });
      var process = function(job){
        assert.equal(producer_dev._event_failed.length, 1);
        assert.equal(job_dev.uuid, job[0].uuid);
        setTimeout(function(){
          producer_dev.offFailed(process);
          assert.equal(producer_dev._event_failed.length, 0);
          done();
        });
      };
      producer_dev.onFailed(process, 1);
    });

    it('should get timeout job form producer', function(done){
      work_dev.onPerform(function(job, feedback){
        assert.equal(job_dev.uuid, job.uuid);
        work_dev.offPerform();
      });
      var process = function(job){
        assert.equal(producer_dev._event_timeout.length, 1);
        assert.equal(job_dev.uuid, job[0].uuid);
        setTimeout(function(){
          producer_dev.offTimeout(process);
          assert.equal(producer_dev._event_timeout.length, 0);
          done();
        });
      };
      producer_dev.onTimeout(process, 3);
    });
  });
});