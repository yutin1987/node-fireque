var assert = require("assert"),
    Fireque = require("./index.js"),
    redis = require("redis"),
    client = redis.createClient();
    queueName = Fireque._getQueueName();

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
  beforeEach(function(done){
    client.flushall(done);
  });

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
          assert.equal(1, getListCount(reply, job_dev.uuid));
          job_dev.dequeue(function(){
            client.lrange(queueName + ':push:queue:med', -100, 100, function(err, reply){
              assert.equal(0, getListCount(reply, job_dev.uuid));
              done();
            });
          });
        });
      });
    });

    it('job should not in the queue low', function(done){
      job_dev.enqueue(function(){
        client.lrange(queueName + ':push:queue:low', -100, 100, function(err, reply){
          assert.equal(1, getListCount(reply, job_dev.uuid));
          job_dev.dequeue(function(){
            client.lrange(queueName + ':push:queue:low', -100, 100, function(err, reply){
              assert.equal(0, getListCount(reply, job_dev.uuid));
              done();
            });
          });
        });
      }, 'low');
    });

    it('job should not in the queue high', function(done){
      job_dev.enqueue(function(){
        client.lrange(queueName + ':push:queue:high', -100, 100, function(err, reply){
          assert.equal(1, getListCount(reply, job_dev.uuid));
          job_dev.dequeue(function(){
            client.lrange(queueName + ':push:queue:high', -100, 100, function(err, reply){
              assert.equal(0, getListCount(reply, job_dev.uuid));
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
          assert.equal(0, getListCount(reply, job_dev.uuid));
          client.lrange(queueName + ':push:queue:high', -100, 100, function(err, reply){
            assert.equal(1, getListCount(reply, job_dev.uuid));
            done();
          });
        });
      }, 'high');
    });

    it('job should in the queue low', function(done){
      job_dev.requeue(function(){
        client.lrange(queueName + ':push:queue:med', -100, 100, function(err, reply){
          assert.equal(0, getListCount(reply, job_dev.uuid));
          client.lrange(queueName + ':push:queue:low', -100, 100, function(err, reply){
            assert.equal(1, getListCount(reply, job_dev.uuid));
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
            assert.equal(1, getListCount(reply, job_dev.uuid));
            done();
          });
        });
    });

    it('job should in the failed', function(done){
      job_dev.failed(function(){
        client.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
          assert.equal(1, getListCount(reply, job_dev.uuid));
          done();
        });
      });
    });
  });
});

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

  describe('#new Work(\'push\', {...})', function(){
    var work_dev = new Fireque.Worker('push', {
      workload: 500,
      wait: 60,
      priority: ['high', 'high'],
      host: '192.168.1.1',
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
    it('host should return 192.168.1.1', function(){
      assert.equal('192.168.1.1', work_dev._connection.host);
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
    
    beforeEach(function(done){
      job_dev.enqueue(done);
    });

    this.timeout(10000);

    it('Completed: job should in completed only 1', function(done){
      var perform = function(job, feedback){
        assert.equal(job_dev.uuid, job.uuid);
        client.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
          assert.equal(work_dev._wrokers.length, 1);
          assert.equal(1, getListCount(reply, job.uuid), 'should in processing only 1');
          feedback(true);
          setTimeout(function(){
            client.lrange(queueName + ':push:completed', -100, 100, function(err, reply){
              assert.equal(1, getListCount(reply, job.uuid), 'should in completed only 1');
              work_dev.offPerform(perform);
              assert.equal(work_dev._wrokers.length, 0);
              done();
            });
          });
        });
      };
      work_dev.onPerform(perform); 
    });

    it('Failed: job should in failed only 1', function(done){
      var perform = function(job, feedback){
        assert.equal(job_dev.uuid, job.uuid);
        client.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
          assert.equal(work_dev._wrokers.length, 1);
          assert.equal(1, getListCount(reply, job.uuid), 'should in processing only 1');
          feedback(false);
          setTimeout(function(){
            client.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
              assert.equal(1, getListCount(reply, job.uuid), 'should in failed only 1');
              work_dev.offPerform(perform);
              assert.equal(work_dev._wrokers.length, 0);
              done();
            });
          });
        });
      };
      work_dev.onPerform(perform); 
    });

    it('Throw: job should in failed only 1', function(done){
        var perform = function(job, feedback){
        assert.equal(job_dev.uuid, job.uuid);
        client.lrange(queueName + ':push:processing', -100, 100, function(err, reply){
          assert.equal(work_dev._wrokers.length, 1);
          assert.equal(1, getListCount(reply, job.uuid), 'should in processing only 1');
          setTimeout(function(){
            client.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
              assert.equal(1, getListCount(reply, job.uuid), 'should in failed only 1');
              work_dev.offPerform(perform);
              assert.equal(work_dev._wrokers.length, 0);
              done();
            });
          });
        });
        throw (new Error('throw error test.'));
      };
      work_dev.onPerform(perform); 
    });
  });
});

describe('Producer', function(){
  beforeEach(function(done){
    client.flushall(done);
  });

  describe('#Producer Completed', function(){
    var job_dev = new Fireque.Job('push', {
        x: 2,
        y: 5
    });
    var work_dev = new Fireque.Worker('push', {
        wait: 1,
        workload: 1
    }, { connection: job_dev._connection});
    var producer_dev = new Fireque.Producer('push', {
      connection: job_dev._connection
    });

    beforeEach(function(done){
      job_dev.enqueue(done);
    });

    this.timeout(10000);

    it('should get completed job form producer', function(done){
      work_dev.onPerform(function(job, feedback){
        assert.equal(job_completed.uuid, job.uuid);
        feedback(true);
      });
      producer_dev.onCompleted(function(job){
        assert.equal(job_completed.uuid, job.uuid);
        done();
      }, 1);
    });
  });
});