var assert = require("assert"),
    Fireque = require("./index.js"),
    redis = require("redis");


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
          var count = 0;
          for (var i = 0; i < reply.length; i++) {
            if ( reply[i] == job_dev.uuid ) {
              count += 1;
            };
          };
          assert.equal(1, count);
          done();
        });
      }, 'low');
    });

    it('job should not in the queue low', function(done){
      job_dev.dequeue(function(){
        connection.lrange(queueName + ':push:queue:low', -100, 100, function(err, reply){
          var count = 0;
          for (var i = 0; i < reply.length; i++) {
            if ( reply[i] == job_dev.uuid ) {
              count += 1;
            };
          };
          assert.equal(0, count);
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
          var count = 0;
          for (var i = 0; i < reply.length; i++) {
            if ( reply[i] == job_dev.uuid ) {
              count += 1;
            };
          };
          assert.equal(1, count);
          done();
        });
      });
    });

    it('job should in the queue high', function(done){
      job_dev.requeue(function(){
        connection.lrange(queueName + ':push:queue:high', -100, 100, function(err, reply){
          var count = 0;
          for (var i = 0; i < reply.length; i++) {
            if ( reply[i] == job_dev.uuid ) {
              count += 1;
            };
          };
          assert.equal(1, count);
          done();
        });
      }, 'high');
    });

    it('job should not in the queue heigh', function(done){
      job_dev.requeue(function(){
        connection.lrange(queueName + ':push:queue:high', -100, 100, function(err, reply){
          var count = 0;
          for (var i = 0; i < reply.length; i++) {
            if ( reply[i] == job_dev.uuid ) {
              count += 1;
            };
          };
          assert.equal(0, count);
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
            var count = 0;
            for (var i = 0; i < reply.length; i++) {
              if ( reply[i] == job_dev.uuid ) {
                count += 1;
              };
            };
            assert.equal(1, count);
            done();
          });
        });
      });
    });

    it('job should in the failed', function(done){
      job_dev.requeue(function(){
        job_dev.failed(function(){
          connection.lrange(queueName + ':push:failed', -100, 100, function(err, reply){
            var count = 0;
            for (var i = 0; i < reply.length; i++) {
              if ( reply[i] == job_dev.uuid ) {
                count += 1;
              };
            };
            assert.equal(1, count);
            done();
          });
        });
      });
    });
  });
});