var assert = require("assert"),
    async = require("async"),
    Fireque = require("../index.js"),
    redis = require("redis"),
    client = redis.createClient(),
    queueName = Fireque._getQueueName();

describe('Monitor', function(){
  beforeEach(function(done){
    client.flushall(done);
  });

  describe('#Monitor Base', function(){
    var monitoer = new Fireque.Monitor('push');

    this.timeout(5000);

    it('prefix should return push', function(){
      assert.equal(monitoer._getPrefix(), queueName + ':push');
    });

    it('_fetchProtectFromBuffer should return true when buffer is null', function(done){
      monitoer._fetchProtectFromBuffer(function(err, protectKey){
        assert.equal(err, true);
        assert.equal(protectKey.length, 0);
        done();
      });
    });

    it('_fetchProtectFromBuffer should return 2 protectKey', function(done){
      async.each([
        {key: 'x:med', val: 'x123'},
        {key: 'y:low', val: 'y123'},
        {key: 'x:high', val: 'x123'}
      ], function (item, cb) {
        client.lpush(monitoer._getPrefix()+':buffer:'+item.key, item.val, cb);
      }, function (err) {
        assert.equal(err, null);
        monitoer._fetchProtectFromBuffer(function(err, protectKey){
          assert.equal(err, null);
          assert.equal(protectKey.length, 2);
          done();
        });
      });
    });

    it('_fetchPriorityByProtect should return 2 priority', function(done){
      monitoer._fetchPriorityByProtect(['xyz','abc'], function (err, priority) {
        assert.equal(err, null);
        assert.equal(priority['xyz'].length, 6);
        assert.equal(priority['abc'].length, 6);
        done();
      });
    });

    it('_filterLowWorkloadByProtect should return 2 workload', function(done){
      async.each([
        {key: 'aaa', val: '5'},
        {key: 'bbb', val: '4'}
      ], function (item, cb) {
        client.set(monitoer._getPrefix()+':workload:'+item.key, item.val, cb);
      }, function (err) {
        assert.equal(err, null);
        monitoer._filterLowWorkloadbyProtect(['aaa','bbb','ccc'], function(err, workload){
          assert.equal(err, null);
          assert.equal(workload[0], 'bbb');
          assert.equal(workload[1], 'ccc');
          done();
        });
      });
    });

    it('_getLicenseByProtect should return true', function(done){
      async.p
      async.each(['ca', 'ca', 'ca', 'ca', 'ca'], function (item, cb) {
        monitoer._getLicenseByProtect(item, cb);
      }, function (err) {
        monitoer._getLicenseByProtect('ca', function (err) {
          assert.equal(err, true);
          done();
        });
      });
    });

    it('_getLicenseByProtect should return null', function(done){
      async.each(['ca', 'ca', 'ca', 'ca', 'ca'], function (item, cb) {
        monitoer._getLicenseByProtect(item, cb);
      }, function (err) {
        async.series([
          function (cb) {
            monitoer._returnLiccenseByProtect('ca', cb);
          },
          function (cb) {
            monitoer._getLicenseByProtect('ca', cb);
          }
        ], function (err) {
          assert.equal(err, null);
          done();
        });
      });
    });

    it('_getUuidFromBufferByProtect should return high med low', function(done){
      async.each(['high', 'med', 'low'], function (item, cb) {
        client.lpush(monitoer._getPrefix() + ':buffer:ca:' + item, 'uuid ' + item, cb);
      }, function (err) {
        assert.equal(err, null);
        async.series([
          function (cb) {
            monitoer._getUuidFromBufferByProtect('ca', cb);
          },
          function (cb) {
            monitoer._getUuidFromBufferByProtect('ca', cb);
          },
          function (cb) {
            monitoer._getUuidFromBufferByProtect('ca', cb);
          }
        ], function (err, result) {
          assert.equal(err, null);
          assert.equal(result[0].priority, 'high');
          assert.equal(result[1].priority, 'med');
          assert.equal(result[2].priority, 'low');
          assert.equal(result[0].uuid, 'uuid high');
          assert.equal(result[1].uuid, 'uuid med');
          assert.equal(result[2].uuid, 'uuid low');
          done();
        });
      });
    });

    it('_getUuidFromBufferByProtect should return low med high', function(done){
      async.each(['high', 'med', 'low'], function (item, cb) {
        client.lpush(monitoer._getPrefix() + ':buffer:ca:' + item, 'uuid ' + item, cb);
      }, function (err) {
        assert.equal(err, null);
        monitoer._priority['ca'] = ['low', 'med', 'med'];
        async.series([
          function (cb) {
            monitoer._getUuidFromBufferByProtect('ca', cb);
          },
          function (cb) {
            monitoer._getUuidFromBufferByProtect('ca', cb);
          },
          function (cb) {
            monitoer._getUuidFromBufferByProtect('ca', cb);
          }
        ], function (err, result) {
          assert.equal(err, null);
          assert.equal(result[2].priority, 'high');
          assert.equal(result[1].priority, 'med');
          assert.equal(result[0].priority, 'low');
          assert.equal(result[2].uuid, 'uuid high');
          assert.equal(result[1].uuid, 'uuid med');
          assert.equal(result[0].uuid, 'uuid low');
          done();
        });
      });
    });

    it('_pushUuidToQueue should return null', function(done){
      monitoer._pushUuidToQueue('1234567890', function(err) {
        assert.equal(err, null);
        client.rpop(monitoer._getPrefix() + ':queue', function(err, reply) {
          assert.equal(err, null);
          assert.equal(reply, '1234567890');
          done();
        });
      });
    });

    it('_popBufferToQueueByProtect should return null', function(done){
      async.each(['high', 'med', 'low'], function (item, cb) {
        client.lpush(monitoer._getPrefix() + ':buffer:ca:' + item, 'uuid ' + item, cb);
      }, function (err) {
        assert.equal(err, null);
        monitoer._priority['ca'] = ['low', 'med', 'med'];
        monitoer._popBufferToQueueByProtect('ca', function(err, task){
          assert.equal(err, null);
          assert.equal(task.uuid, 'uuid low');
          assert.equal(task.priority, 'low');
          assert.equal(monitoer._priority['ca'].length, 2);
          client.rpop(monitoer._getPrefix() + ':queue', function(err, reply){
            assert.equal(err, null);
            assert.equal(reply, 'uuid low');
            done();
          });
        });
      });
    });

    it('_listenBuffer should return 3 high', function(done){
      async.each(['ca1', 'ca2', 'ca3'], function (item, cb) {
        client.lpush(monitoer._getPrefix() + ':buffer:' +item+ ':high', 'uuid ' + item, cb);
      }, function (err) {
        assert.equal(err, null);
        monitoer._listenBuffer(function(err, result){
          assert.equal(err, null);
          assert.equal(result.length, 3);
          done();
        });
      });
    });
  });
});