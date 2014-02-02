var assert = require("assert"),
    Fireque = require("./index.js"),
    redis = require("redis");

var getListCount = function (list, value) {
  var count = 0;
  for (var i = 0; i < list.length; i++) {
    if ( list[i] === value ) {
      count += 1;
    };
  };
  return count;
}


describe('Job', function(){
  describe('#new Job(null, {}, {port: 9000, host: 192.168.0.1})', function(){
    var job_dev = new Fireque.Job(null, '', {'port': '9000', 'host': '192.168.0.1'});
    it('post should return 9000', function(){
      assert.equal('9000', job_dev._connection.port);
    })
    it('host should return 192.168.0.1', function(){
      assert.equal('192.168.0.1', job_dev._connection.host);
    })
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
    var connection = job_dev._connection;
    var queueName = Fireque._getQueueName();
    it('redis should has data', function(done){
      job_dev.enqueue(function(err, job_obj){
          assert.equal(err, null);
          connection.hexists(queueName + ':job:' + job_dev.uuid, 'data', function(err, reply){
            assert.equal(1, reply);
            done();
          });
      });
    });
    it('redis.timeout should return 60', function(done){
      connection.hget(queueName + ':job:' + job_dev.uuid, 'timeout', function(err, reply){
        assert.equal(60, reply);
        done();
      });
    });
  });

  describe('#dequeue Job', function(){
    var job_dev = new Fireque.Job('push',{
        name: 'fireque'
    });
    var connection = job_dev._connection;
    var queueName = Fireque._getQueueName();
    it('job should in the queue low', function(done){
      job_dev.enqueue(function(){
        connection.lrange(queueName + ':push:queue:low', -100, 100, function(err, reply){
          assert.equal(1, getListCount(reply, job_dev.uuid));
          done();
        });
      }, 'low');
    });

    it('job should not in the queue low', function(done){
      job_dev.dequeue(function(){
        connection.lrange(queueName + ':push:queue:low', -100, 100, function(err, reply){
          assert.equal(0, getListCount(reply, job_dev.uuid));
          done();
        });
      });
    });
  });

  describe('#requeue Job', function(){
    var job_dev = new Fireque.Job('push',{
        name: 'fireque'
    });
    var connection = job_dev._connection;
    var queueName = Fireque._getQueueName();
    it('job should in the queue med', function(done){
      job_dev.enqueue(function(){
        connection.lrange(queueName + ':push:queue:med', -100, 100, function(err, reply){
          assert.equal(1, getListCount(reply, job_dev.uuid));
          done();
        });
      });
    });

    it('job should in the queue high', function(done){
      job_dev.requeue(function(){
        connection.lrange(queueName + ':push:queue:high', -100, 100, function(err, reply){
          assert.equal(1, getListCount(reply, job_dev.uuid));
          done();
        });
      }, 'high');
    });

    it('job should not in the queue heigh', function(done){
      job_dev.requeue(function(){
        connection.lrange(queueName + ':push:queue:high', -100, 100, function(err, reply){
          assert.equal(0, getListCount(reply, job_dev.uuid));
          done();
        });
      }, 'low');
    });
  });

  describe('#operate Job', function(){
    var job_dev = new Fireque.Job('push', {
        name: 'fireque'
    });
    var connection = job_dev._connection;
    var queueName = Fireque._getQueueName();

    it('job should in the completed', function(done){
      job_dev.enqueue(function(){
        job_dev.completed(function(){
          connection.lrange(queueName + ':push:completed', -100, 100, function(err, reply){
            assert.equal(1, getListCount(reply, job_dev.uuid));
            done();
          });
        });
      });
    });

    it('job should in the failed', function(done){
      job_dev.requeue(function(){
        job_dev.failed(function(){
          connection.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
            assert.equal(1, getListCount(reply, job_dev.uuid));
            done();
          });
        });
      });
    });
  });
});

describe('Work', function(){
  describe('#new Work()', function(){
    var work_dev = new Fireque.Work();
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

  describe('#new Work(\'push\', {...})', function(){
    var work_dev = new Fireque.Work('push', {
      workload: 500,
      wait: 60,
      priority: ['high', 'high'],
      host: '192.168.0.1',
      port: 9000
    });
    it('protocol should return universal', function(){
      assert.equal('push', work_dev.protocol);
    });
    it('workload should return 500', function(){
      assert.equal(500, work_dev.workload);
    });
    it('wait should return 60', function(){
      assert.equal(60, work_dev._wait);
    });
    it('priority.length should return 2', function(){
      assert.equal(2, work_dev._priority.length);
    });
    it('port should return 9000', function(){
      assert.equal(9000, work_dev._connection.port);
    });
    it('host should return 192.168.0.1', function(){
      assert.equal('192.168.0.1', work_dev._connection.host);
    });
  });

  describe('#Work Perform', function(){
    var work_dev = new Fireque.Work('push', {
        wait: 1,
        workload: 1
    });
    var connection = work_dev._connection;
    var queueName = Fireque._getQueueName();

    this.timeout(30000);
    var job_completed = new Fireque.Job('push', {
        x: 2,
        y: 5
    }, { connection: work_dev._connection});
    it('Completed: queue should in processing only 1', function(done){
      job_completed.enqueue(function(){
        work_dev.perform(function(job, feedback){
          assert.equal('push', job.protocol);
          connection.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
            assert.equal(1, getListCount(reply, job.uuid));
            feedback(true);
            job_completed = job;
            done();
          });
        });
      });
    });
    it('Completed: queue should in completed only 1', function(done){
      var ready = 2;
      connection.lrange(queueName + ':push:completed', -100, 100, function(err, reply){
        assert.equal(1, getListCount(reply, job_completed.uuid));
        ready -= 1;
        if ( ready < 1 ){
          done();
        }
      });
      connection.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
        assert.equal(0, getListCount(reply, job_completed.uuid));
        ready -= 1;
        if ( ready < 1 ){
          done();
        }
      });
    });

    var job_failed = new Fireque.Job('push', {
        x: 2,
        y: 5
    }, { connection: work_dev._connection});
    it('Failed: queue should in processing only 1', function(done){
      job_failed.enqueue(function(){
        work_dev.perform(function(job, feedback){
          assert.equal('push', job.protocol);
          connection.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
            assert.equal(1, getListCount(reply, job.uuid));
            feedback(false);
            job_failed = job;
            done();
          });
        });
      });
    });
    it('Failed: queue should in failed only 1', function(done){
      var ready = 2;
      connection.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
        assert.equal(1, getListCount(reply, job_failed.uuid));
        ready -= 1;
        if ( ready < 1 ){
          done();
        }
      });
      connection.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
        assert.equal(0, getListCount(reply, job_failed.uuid));
        ready -= 1;
        if ( ready < 1 ){
          done();
        }
      });
    });

    var job_throw = new Fireque.Job('push', {
        x: 2,
        y: 5
    }, { connection: work_dev._connection});
    it('Throw: queue should in processing only 1', function(done){
      job_throw.enqueue(function(){
        work_dev.perform(function(job, feedback){
          assert.equal('push', job.protocol);
          connection.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
            assert.equal(1, getListCount(reply, job.uuid));
            job_throw = job;
            setTimeout(done, 1000);
          });
          throw (new Error('throw error test.'));
        });
      });
    });
    it('Throw: queue should in failed only 1', function(done){
      var ready = 2;
      connection.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
        assert.equal(1, getListCount(reply, job_throw.uuid));
        ready -= 1;
        if ( ready < 1 ){
          done();
        }
      });
      connection.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
        assert.equal(0, getListCount(reply, job_throw.uuid));
        ready -= 1;
        if ( ready < 1 ){
          done();
        }
      });
    });
  });
});

describe('Producer', function(){
  describe('#Producer Completed', function(){
    var work_dev = new Fireque.Work('push', {
        wait: 1,
        workload: 1
    });
    var producer_completed = new Fireque.Producer('push', {
      connection: work_dev._connection
    });
    var job_completed = new Fireque.Job('push', {
        x: 2,
        y: 5
    }, { connection: work_dev._connection});
    var connection = work_dev._connection;
    var queueName = Fireque._getQueueName();

    this.timeout(30000);
    it('Completed: queue should in processing only 1', function(done){
      connection.flushall(function(err, reply){
        job_completed.enqueue(function(){
          work_dev.perform(function(job, feedback){
            assert.equal('push', job.protocol);
            assert.equal(job_completed.uuid, job.uuid);
            connection.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
              assert.equal(1, getListCount(reply, job.uuid));
              feedback(true);
              done();
            });
          });
        });
      });
    });

    it('Completed: queue should in completed only 1', function(done){
      connection.lrange(queueName + ':push:completed', -100, 100, function(err, reply){
        assert.equal(1, getListCount(reply, job_completed.uuid));
        done();
      });
    });

    it('Completed: producer should get completed', function(done){
      producer_completed.onCompleted(function(job){
        assert.equal(job_completed.uuid, job[0].uuid);
        connection.lrange(queueName + ':push:completed', -100, 100, function(err, reply){
          assert.equal(0, getListCount(reply, job_completed.uuid));
          done();
        });
      }, 1);
    });
  });
  describe('#Producer Failed', function(){
    var work_dev = new Fireque.Work('push', {
        wait: 1,
        workload: 1
    });
    var producer_failed = new Fireque.Producer('push', {
      connection: work_dev._connection
    });
    var job_failed = new Fireque.Job('push', {
        x: 2,
        y: 5
    }, { connection: work_dev._connection});
    var connection = work_dev._connection;
    var queueName = Fireque._getQueueName();
    
    this.timeout(30000);
    it('Failed: queue should in processing only 1', function(done){
      connection.flushall(function(err, reply){
        job_failed.enqueue(function(){
          work_dev.perform(function(job, feedback){
            assert.equal('push', job.protocol);
            assert.equal(job_failed.uuid, job.uuid);
            connection.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
              assert.equal(1, getListCount(reply, job.uuid));
              feedback(false);
              done();
            });
          });
        });
      });
    });

    it('Failed: queue should in failed only 1', function(done){
      connection.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
        assert.equal(1, getListCount(reply, job_failed.uuid));
        done();
      });
    });

    it('Failed: producer should get failed', function(done){
      producer_failed.onFailed(function(job){
        assert.equal(job_failed.uuid, job[0].uuid);
        connection.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
          assert.equal(0, getListCount(reply, job_failed.uuid));
          done();
        });
      }, 1);
    });
  });
});