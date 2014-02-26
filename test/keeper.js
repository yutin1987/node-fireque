var assert = require("assert"),
    async = require("async"),
    Fireque = require("../index.js"),
    redis = require("redis"),
    model = require("../lib/model.js"),
    client = redis.createClient();

var obj = Fireque._apply({});
obj.protocol = 'universal';

describe('Monitor', function(){
  beforeEach(function(done){
    client.flushall(done);
  });

  describe('#Monitor Base', function(){
    var keeper = new Fireque.Keeper();

    this.timeout(5000);

    it('_fetchPriorityByProtect should return 2 priority', function(done){
      keeper._fetchPriorityByProtect(['xyz','abc'], function (err, priority) {
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
        client.set(keeper._getPrefix()+':workload:'+item.key, item.val, cb);
      }, function (err) {
        assert.equal(err, null);
        keeper._filterLowWorkloadbyProtect(['aaa','bbb','ccc'], function(err, workload){
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
        keeper._getLicenseByProtect(item, cb);
      }, function (err) {
        keeper._getLicenseByProtect('ca', function (err) {
          assert.equal(err, true);
          done();
        });
      });
    });

    it('_getLicenseByProtect should return null', function(done){
      async.each(['ca', 'ca', 'ca', 'ca', 'ca'], function (item, cb) {
        keeper._getLicenseByProtect(item, cb);
      }, function (err) {
        async.series([
          function (cb) {
            keeper._returnLiccenseByProtect('ca', cb);
          },
          function (cb) {
            keeper._getLicenseByProtect('ca', cb);
          }
        ], function (err) {
          assert.equal(err, null);
          done();
        });
      });
    });

    it('_pushUuidToQueue should return null', function(done){
      keeper._pushUuidToQueue('1234567890', function(err) {
        assert.equal(err, null);
        client.rpop(keeper._getPrefix() + ':queue', function(err, reply) {
          assert.equal(err, null);
          assert.equal(reply, '1234567890');
          done();
        });
      });
    });

    it('_popBufferToQueueByProtect should return null', function(done){
      async.each(['high', 'med', 'low'], function (item, cb) {
        model.pushToBufferByProtect.bind(obj)('ca', 'uuid ' + item, item, cb);
      }, function (err) {
        assert.equal(err, null);
        keeper._priority['ca'] = ['low', 'med', 'med'];
        keeper._popBufferToQueueByProtect('ca', function(err, task){
          assert.equal(err, null);
          assert.equal(task.uuid, 'uuid low');
          assert.equal(task.from, 'low');
          assert.equal(keeper._priority['ca'].length, 2);
          model.popFromQueue.bind(obj)( function (err, uuid, from) {
            assert.equal(err, null);
            assert.equal(uuid, 'uuid low');
            assert.equal(from, null);
            done();
          })
        });
      });
    });

    it('_listenBuffer should return 3 high', function(done){
      async.each(['ca1', 'ca2', 'ca3'], function (item, cb) {
        model.pushToBufferByProtect.bind(obj)(item, 'uuid '+item, cb);
      }, function (err) {
        assert.equal(err, null);
        keeper._listenBuffer(function(err, result){
          assert.equal(err, null);
          assert.equal(result.length, 3);
          done();
        });
      });
    });
  });
});