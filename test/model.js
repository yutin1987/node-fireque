var assert = require("assert"),
    uid = require('node-uuid'),
    async = require("async"),
    model = require("../lib/model.js"),
    redis = require("redis"),
    getListCount = require("./getListCount.js"),
    Fireque = require("../index.js"),
    client = redis.createClient();


var obj = Fireque._apply({});
obj.protectKey = 'unrestricted';

describe('Library Model', function(){
    beforeEach(function(done){
        client.flushall(done);
    });

    var uuid = [uid.v4(), uid.v4(), uid.v4()];
  
    describe('Queue', function () {
        it('pushToQueue', function (done) {
            model.pushToQueue.bind(obj)(uuid[0], function (err) {
                assert.equal(err, null);
                client.lrange( obj._getPrefixforProtocol() + ':queue', -100, 100, function (err, reply) {
                    assert.equal(err, null);
                    assert.equal(getListCount(reply, uuid[0]), 1);
                    done();
                });
            });
        });
        it('popFromQueue', function (done) {
            async.eachSeries(uuid, function (uuid, cb) {
                model.pushToQueue.bind(obj)(uuid, cb);
            }, function (err) {
                model.popFromQueue.bind(obj)( function (err, queue_uuid, from) {
                    assert.equal(err, null);
                    assert.equal(queue_uuid, uuid[0]);
                    assert.equal(from, null);
                    done();
                });
            });
        });
        it('popFromQueue use focus', function (done) {
            async.eachSeries(uuid, function (uuid, cb) {
                model.pushToQueue.bind(obj)(uuid, true, cb);
            }, function (err) {
                model.popFromQueue.bind(obj)( function (err, queue_uuid, from) {
                    assert.equal(err, null);
                    assert.equal(queue_uuid, uuid[2]);
                    assert.equal(from, null);
                    done();
                });
            });
        });
        it('popFromQueue by priority', function (done) {
            async.each([':buffer:unrestricted:high', ':buffer:unrestricted:med', ':buffer:unrestricted:low'], function (item, cb) {
                client.lpush(obj._getPrefixforProtocol() + item, 'yutin', cb);
            }, function (err) {
                model.popFromQueue.bind(obj)(['high','med','low'], function (err, uuid, from) {
                    assert.equal(err, null);
                    assert.equal(uuid, 'yutin');
                    assert.equal(from, 'high');
                    done();
                });
            });
        });
        it('lenOfQueue', function (done) {
            async.eachSeries(uuid, function (uuid, cb) {
                model.pushToQueue.bind(obj)(uuid, cb);
            }, function (err) {
                model.lenOfQueue.bind(obj)( function (err, len) {
                    assert.equal(err, null);
                    assert.equal(len, 3);
                    done();
                });
            });
        });
    });

    describe('Completed', function () {
        it('pushToCompleted', function (done) {
            model.pushToCompleted.bind(obj)(uuid[0], function (err) {
                assert.equal(err, null);
                client.lrange( obj._getPrefixforProtocol() + ':completed', -100, 100, function (err, reply) {
                    assert.equal(err, null);
                    assert.equal(getListCount(reply, uuid[0]), 1);
                    done();
                });
            });
        });
        it('popFromCompleted', function (done) {
            async.eachSeries(uuid, function (item, cb) {
                model.pushToCompleted.bind(obj)(item, cb);
            }, function (err) {
                assert.equal(err, null);
                async.eachSeries([uuid[2],uuid[1],uuid[0]], function (item, cb) {
                    model.popFromCompleted.bind(obj)(function (err, completed_uuid) {
                        assert.equal(completed_uuid, item);
                        cb(err);
                    });
                }, function (err) {
                    assert.equal(err, null);
                    done();
                });
            });
        });
        it('lenOfCompleted', function (done) {
            async.eachSeries(uuid, function (uuid, cb) {
                model.pushToCompleted.bind(obj)(uuid, cb);
            }, function (err) {
                model.lenOfCompleted.bind(obj)( function (err, len) {
                    assert.equal(err, null);
                    assert.equal(len, 3);
                    done();
                });
            });
        });
    });

    describe('Failed', function () {
        it('pushToFailed', function (done) {
            model.pushToFailed.bind(obj)(uuid[0], function (err) {
                assert.equal(err, null);
                client.lrange( obj._getPrefixforProtocol() + ':failed', -100, 100, function (err, reply) {
                    assert.equal(err, null);
                    assert.equal(getListCount(reply, uuid[0]), 1);
                    done();
                });
            });
        });
        it('popFromFailed', function (done) {
            async.eachSeries(uuid, function (item, cb) {
                model.pushToFailed.bind(obj)(item, cb);
            }, function (err) {
                assert.equal(err, null);
                async.eachSeries([uuid[2],uuid[1],uuid[0]], function (item, cb) {
                    model.popFromFailed.bind(obj)(function (err, failed_uuid) {
                        assert.equal(failed_uuid, item);
                        cb(err);
                    });
                }, function (err) {
                    assert.equal(err, null);
                    done();
                });
            });
        });
        it('lenOfFailed', function (done) {
            async.eachSeries(uuid, function (item, cb) {
                model.pushToFailed.bind(obj)(item, cb);
            }, function (err) {
                model.lenOfFailed.bind(obj)( function (err, len) {
                    assert.equal(err, null);
                    assert.equal(len, 3);
                    done();
                });
            });
        });
    });

    describe('Processing', function () {
        it('pushToProcessing', function (done) {
            model.pushToProcessing.bind(obj)(uuid[0], function (err) {
                assert.equal(err, null);
                client.lrange( obj._getPrefixforProtocol() + ':processing', -100, 100, function (err, reply) {
                    assert.equal(err, null);
                    assert.equal(getListCount(reply, uuid[0]), 1);
                    done();
                });
            });
        });
        it('rmUuidOfProcessing', function (done) {
            async.each(uuid, function (item, cb) {
                model.pushToProcessing.bind(obj)(item, cb);
            }, function (err) {
                assert.equal(err, null);
                model.rmUuidOfProcessing.bind(obj)(uuid[1], function (err, count) {
                    assert.equal(err, null);
                    assert.equal(count, 1);
                    client.lrange( obj._getPrefixforProtocol() + ':processing', -100, 100, function (err, reply) {
                        assert.equal(err, null);
                        assert.equal(getListCount(reply, uuid[1]), 0);
                        assert.equal(getListCount(reply, uuid[0]), 1);
                        assert.equal(getListCount(reply, uuid[2]), 1);
                        done();
                    });
                });
            });
        });
        it('existsUuidInProcessing', function (done) {
            async.each([uuid[0], uuid[1]], function (item, cb) {
                model.pushToProcessing.bind(obj)(item, cb);
            }, function (err) {
                model.existsUuidInProcessing.bind(obj)(uuid[1], function (err, exist) {
                    assert.equal(err, null);
                    assert.equal(exist, true);
                    model.existsUuidInProcessing.bind(obj)(uuid[3], function (err, exist) {
                        assert.equal(err, null);
                        assert.equal(exist, false);
                        done();
                    });
                });
            });
        });
        it('lenOfProcessing', function (done) {
            async.each(uuid, function (item, cb) {
                model.pushToProcessing.bind(obj)(item, cb);
            }, function (err) {
                model.lenOfProcessing.bind(obj)(function (err, len) {
                    assert.equal(err, null);
                    assert.equal(len, 3);
                    done();
                });
            });
        });
    });

    describe('Job', function () {
        it('setJob', function (done) {
            model.setJob.bind(obj)(uuid[0], {protocol: 'push', data: {name: "I'm Job."}}, function (err) {
                assert.equal(err, null);
                async.map(['protocol', 'data'], function (item, cb) {
                    client.hget( obj._getPrefix() + ':job:' + uuid[0], item, cb);
                }, function (err, result) {
                    assert.equal(err, null);
                    assert.equal(result[0], 'push');
                    assert.equal(result[1], JSON.stringify({name: "I'm Job."}));
                    done();
                });
            });
        });
        it('getJob', function (done) {
            model.setJob.bind(obj)(uuid[0], {protocol: 'push', data: {name: "I'm Job."}}, function (err) {
                assert.equal(err, null);
                model.getJob.bind(obj)(uuid[0], function (err, obj) {
                    assert.equal(err, null);
                    assert.equal(obj.protocol, 'push');
                    assert.equal(obj.data.name, "I'm Job.");
                    done();
                });
            });
        });
        it('existsUuidInProcessing', function (done) {
            model.setJob.bind(obj)(uuid[0], {protocol: 'push', data: {name: "I'm Job."}}, function (err) {
                assert.equal(err, null);
                model.expireJob.bind(obj)(uuid[0], function (err) {
                    assert.equal(err, null);
                    client.ttl( obj._getPrefix() + ':job:' + uuid[0], function (err, reply) {
                        assert.equal(err, null);
                        assert.equal(reply > 0, true);
                        done();
                    });
                });
            });
        });
        it('cleanJob', function (done) {
            var queue = [ ':queue', ':completed', ':failed', ':buffer:' + obj.protectKey + ':high', ':buffer:' + obj.protectKey + ':med', ':buffer:' + obj.protectKey + ':low' ];
            async.each(queue, function (item, cb) {
                client.lpush( obj._getPrefixforProtocol() + item, uuid[0], cb);
            }, function (err) {
                assert.equal(err, null);
                model.cleanJob.bind(obj)(uuid[0], function (err, count) {
                    assert.equal(err, null);
                    assert.equal(count, queue.length);
                    done();
                });
            });
        });

        it('delJob', function (done) {
            var key = [ ':job:' + uuid[0], ':job:' + uuid[0] + ':timeout'];
            async.each(key, function (item, cb) {
                client.set( obj._getPrefix() + item, uuid[0], cb);
            }, function (err) {
                model.delJob.bind(obj)(uuid[0], function (err, reply){
                    assert.equal(err, null);
                    async.map(key, function (item, cb) {
                        client.exists( obj._getPrefix() + item, cb);
                    }, function (err, result) {
                        for (var i = result.length - 1; i > -1; i-=1) {
                            assert.equal(result[i], 0);
                        };
                        done();
                    });
                });
            });
        });

        it('setTimeoutOfJob', function (done) {
            model.setTimeoutOfJob.bind(obj)(uuid[0], 30, function(err, job) {
                assert.equal(err, null);
                client.ttl(obj._getPrefix() + ':job:' + uuid[0] + ':timeout', function(err, reply){
                    assert.equal(err, null);
                    assert.equal(reply > 0, true);
                    done();
                });
            });
        });
    });

    describe('Buffer', function () {
        it('pushToBufferByProtect', function (done) {
            async.parallel([
                function (cb) {
                    model.pushToBufferByProtect.bind(obj)('message', uuid[0], 'high', cb);
                },
                function (cb) {
                    model.pushToBufferByProtect.bind(obj)('message', uuid[1], cb);
                },
                function (cb) {
                    model.pushToBufferByProtect.bind(obj)('message', uuid[2], 'low', cb);
                }
            ], function (err, result) {
                async.map(['high', 'med', 'low'], function (item, cb) {
                    client.lrange( obj._getPrefixforProtocol() + ':buffer:message:' + item, -100, 100, cb);
                }, function (err, result) {
                    assert.equal(err, null);
                    assert.equal(getListCount(result[0], uuid[0]), 1);
                    assert.equal(getListCount(result[1], uuid[1]), 1);
                    assert.equal(getListCount(result[2], uuid[2]), 1);
                    done();
                });
            });
        });
        it('popFromBufferByProtect', function (done) {
            async.parallel([
                function (cb) {
                    model.pushToBufferByProtect.bind(obj)('message', uuid[0], 'high', cb);
                },
                function (cb) {
                    model.pushToBufferByProtect.bind(obj)('message', uuid[1], cb);
                },
                function (cb) {
                    model.pushToBufferByProtect.bind(obj)('message', uuid[2], 'low', cb);
                }
            ], function (err) {
                assert.equal(err, null);
                async.series([
                    function (cb) {
                        model.popFromBufferByProtect.bind(obj)('message', 'low', function (err, buffer_uuid, from) {
                            assert.equal(buffer_uuid, uuid[2]);
                            assert.equal(from, 'low');
                            cb(err);
                        });
                    },
                    function (cb) {
                        model.popFromBufferByProtect.bind(obj)('message', function (err, buffer_uuid, from) {
                            assert.equal(buffer_uuid, uuid[0]);
                            assert.equal(from, 'high');
                            cb(err);
                        });
                    },
                    function (cb) {
                        model.popFromBufferByProtect.bind(obj)('message', function (err, buffer_uuid, from) {
                            assert.equal(buffer_uuid, uuid[1]);
                            assert.equal(from, 'med');
                            cb(err);
                        });
                    }
                ], function (err) {
                    assert.equal(err, null);
                    done();
                });
            });
        });
        it('fetchProtectFromBuffer', function (done) {
            async.each(['xxx:high', 'yyy:high', 'xxx:low'], function (item, cb) {
                client.lpush( obj._getPrefixforProtocol() + ':buffer:' + item, '1234567890', cb);
            }, function (err) {
                assert.equal(err, null);
                model.fetchProtectFromBuffer.bind(obj)( function (err, protect) {
                    assert.equal(err, null);
                    assert.equal(protect.length, 2);
                    done();
                });
            });
        });
    });

    describe('Workload', function () {
        it('incrementWorkload', function (done) {
            async.map(['xxx','yyy','zzz'], function (item, cb) {
                model.incrementWorkload.bind(obj)(item, cb);
            }, function (err, result) {
                assert.equal(err, null);
                assert.equal(result[0], 1);
                assert.equal(result[1], 1);
                assert.equal(result[2], 1);
                done();
            });
        });
        it('decrementWorkload', function (done) {
            async.each(['xxx','yyy','yyy','zzz','zzz','iii'], function (item, cb) {                
                model.incrementWorkload.bind(obj)(item, cb);
            }, function (err, result) {
                assert.equal(err, null);
                async.map(['xxx','yyy','zzz','iii'], function (item, cb) {
                    client.zscore( obj._getPrefixforProtocol() + ':workload', item, cb);
                }, function (err, result) {
                    assert.equal(err, null);
                    assert.equal(result[0], 1);
                    assert.equal(result[1], 2);
                    assert.equal(result[2], 2);
                    assert.equal(result[3], 1);
                    async.each(['yyy', 'iii'], function (item, cb) {
                        model.decrementWorkload.bind(obj)(item, cb);
                    }, function (err, result) {
                        assert.equal(err, null);
                        async.map(['xxx','yyy','zzz','iii'], function (item, cb) {
                            client.zscore( obj._getPrefixforProtocol() + ':workload', item, cb);
                        }, function (err, result) {
                            assert.equal(err, null);
                            assert.equal(result[0], 1);
                            assert.equal(result[1], 1);
                            assert.equal(result[2], 2);
                            assert.equal(result[3], 0);
                            done();
                        });
                    });
                });
            });
        });
        it('fetchOverWorkload', function (done) {
            async.each(['xxx','yyy','yyy','zzz','zzz','zzz','iii'], function (item, cb) {                
                model.incrementWorkload.bind(obj)(item, cb);
            }, function (err, result) {
                assert.equal(err, null);
                model.fetchOverWorkload.bind(obj)(3, function (err, reply) {
                    assert.equal(err, null);
                    assert.equal(reply['zzz'], 3);
                    done();
                });
            });
        });
    });

    describe('Schedule', function () {
        it('pushToSchedule', function (done) {
            model.getTime.bind(obj)( function (err, timestamp) {
                assert.equal(err, null);
                assert.equal(timestamp > 0, true);
                async.each([[uuid[0], timestamp], [uuid[1], timestamp], [uuid[2], timestamp + 10]], function (item, cb) {
                    model.pushToSchedule.bind(obj)(item[1], item[0], cb);
                }, function (err) {
                    assert.equal(err, null);
                    async.map([timestamp, timestamp+10], function (item, cb) {
                        client.llen( obj._getPrefixforProtocol() + ':schedule:' + item, cb);
                    }, function (err, result) {
                        assert.equal(err, null);
                        assert.equal(result[0], 2);
                        assert.equal(result[1], 1);
                        done();
                    });
                });
            });
        });
        it('fetchScheduleByTimestamp', function (done) {
            model.getTime.bind(obj)( function (err, timestamp) {
                assert.equal(err, null);
                assert.equal(timestamp > 0, true);
                async.each([[uuid[0], timestamp - 10], [uuid[1], timestamp], [uuid[2], timestamp + 10]], function (item, cb) {
                    model.pushToSchedule.bind(obj)(item[1], item[0], cb);
                }, function (err) {
                    assert.equal(err, null);
                    model.fetchScheduleByTimestamp.bind(obj)(timestamp, function (err, keys) {
                        assert.equal(err, null);
                        assert.equal(keys.length, 2);
                        done();
                    });
                });
            });
        });
        it('popUuidFromSchedule', function (done) {
            model.getTime.bind(obj)( function (err, timestamp) {
                assert.equal(err, null);
                assert.equal(timestamp > 0, true);
                async.eachSeries([[uuid[0], timestamp], [uuid[1], timestamp], [uuid[2], timestamp + 10]], function (item, cb) {
                    model.pushToSchedule.bind(obj)(item[1], item[0], cb);
                }, function (err) {
                    assert.equal(err, null);
                    async.map([timestamp, timestamp + 10, timestamp + 10], function (item, cb) {
                        model.popUuidFromSchedule.bind(obj)(item, cb);
                    }, function (err, result) {
                        assert.equal(err, null);
                        assert.equal(result[0], uuid[0]);
                        assert.equal(result[1], uuid[2]);
                        assert.equal(result[2], null);
                        done();
                    });
                });
            });
        });
    });
});