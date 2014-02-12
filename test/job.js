var assert = require("assert"),
    yt = require("yutin-node"),
    Fireque = require("../index.js"),
    redis = require("redis"),
    client = redis.createClient(),
    queueName = Fireque._getQueueName();

describe('Job', function(){
  beforeEach(function(done){
    client.flushall(done);
  });

  describe('#new Job()', function(){
    var job_dev = new Fireque.Job();
    it('protocol should return universal', function(){
      assert.equal('universal', job_dev.protocol);
    })
    it('data should return ""', function(){
      assert.equal('', job_dev.data);
    })
    it('timeout should return 30', function(){
      assert.equal(30, job_dev.timeout);
    })
  });

  describe('#new Job(\'push\', {name:\'fireque\'}, {timeout: 60})', function(){
    var job_dev = new Fireque.Job('push',{
        name: 'fireque'
    },{
        timeout: 60
    });
    it('protocol should return push', function(){
      assert.equal('push', job_dev.protocol);
    })
    it('data.name should return fireque', function(){
      assert.equal('fireque', job_dev.data.name);
    })
    it('timeout should return 60', function(){
      assert.equal(60, job_dev.timeout);
    })
  });

  describe('#enqueue Job', function(){
    var job_dev = new Fireque.Job('push',{
        name: 'fireque'
    },{
        timeout: 60
    });

    beforeEach(function(done){
      job_dev.enqueue(done);
    });

    it('redis should has data', function(done){
      client.hexists(queueName + ':job:' + job_dev.uuid, 'data', function(err, reply){
        assert.equal(1, reply);
        done();
      });
    });

    it('redis.timeout should return 60', function(done){
      client.hget(queueName + ':job:' + job_dev.uuid, 'timeout', function(err, reply){
        assert.equal(60, reply);
        done();
      });
    });

  });

  describe('#dequeue Job', function(){
    var job_dev = new Fireque.Job('push',{
        name: 'fireque'
    });

    it('job should not in the queue med', function(done){
      job_dev.enqueue(function(){
        client.lrange(queueName + ':push:queue:med', -100, 100, function(err, reply){
          assert.equal(1, yt.getListCount(reply, job_dev.uuid));
          job_dev.dequeue(function(){
            client.lrange(queueName + ':push:queue:med', -100, 100, function(err, reply){
              assert.equal(0, yt.getListCount(reply, job_dev.uuid));
              done();
            });
          });
        });
      });
    });

    it('job should not in the queue low', function(done){
      job_dev.enqueue(function(){
        client.lrange(queueName + ':push:queue:low', -100, 100, function(err, reply){
          assert.equal(1, yt.getListCount(reply, job_dev.uuid));
          job_dev.dequeue(function(){
            client.lrange(queueName + ':push:queue:low', -100, 100, function(err, reply){
              assert.equal(0, yt.getListCount(reply, job_dev.uuid));
              done();
            });
          });
        });
      }, 'low');
    });

    it('job should not in the queue high', function(done){
      job_dev.enqueue(function(){
        client.lrange(queueName + ':push:queue:high', -100, 100, function(err, reply){
          assert.equal(1, yt.getListCount(reply, job_dev.uuid));
          job_dev.dequeue(function(){
            client.lrange(queueName + ':push:queue:high', -100, 100, function(err, reply){
              assert.equal(0, yt.getListCount(reply, job_dev.uuid));
              done();
            });
          });
        });
      }, 'high');
    });
  });

  describe('#requeue Job', function(){
    var job_dev = new Fireque.Job('push',{
        name: 'fireque'
    });

    beforeEach(function(done){
      job_dev.enqueue(done);
    });

    it('job should in the queue high', function(done){
      job_dev.requeue(function(){
        client.lrange(queueName + ':push:queue:med', -100, 100, function(err, reply){
          assert.equal(0, yt.getListCount(reply, job_dev.uuid));
          client.lrange(queueName + ':push:queue:high', -100, 100, function(err, reply){
            assert.equal(1, yt.getListCount(reply, job_dev.uuid));
            done();
          });
        });
      }, 'high');
    });

    it('job should in the queue low', function(done){
      job_dev.requeue(function(){
        client.lrange(queueName + ':push:queue:med', -100, 100, function(err, reply){
          assert.equal(0, yt.getListCount(reply, job_dev.uuid));
          client.lrange(queueName + ':push:queue:low', -100, 100, function(err, reply){
            assert.equal(1, yt.getListCount(reply, job_dev.uuid));
            done();
          });
        });
      }, 'low');
    });
  });

  describe('#operate Job', function(){
    var job_dev = new Fireque.Job('push', {
        name: 'fireque'
    });

    beforeEach(function(done){
      job_dev.enqueue(done);
    });

    it('job should in the completed', function(done){
        job_dev.completed(function(){
          client.lrange(queueName + ':push:completed', -100, 100, function(err, reply){
            assert.equal(1, yt.getListCount(reply, job_dev.uuid));
            done();
          });
        });
    });

    it('job should in the failed', function(done){
      job_dev.failed(function(){
        client.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
          assert.equal(1, yt.getListCount(reply, job_dev.uuid));
          done();
        });
      });
    });
  });
});

