var assert = require("assert"),
    async = require("async"),
    getListCount = require("./getListCount.js"),
    Fireque = require("../index.js"),
    model = require("../lib/model.js"),
    redis = require("redis"),
    client = redis.createClient();

describe('Job', function(){

    var time = new Date();
    var timestamp = parseInt(time.getTime() / 1000);

    var jobs = [];

    for (var i = 0; i < 10; i++) {
        jobs.push(Fireque.Job(null, {name: 'fireque', num: i}));
    };

    var obj = Fireque._apply({});
    obj.protectKey = 'unrestricted';
    obj.protocol = 'universal';

    beforeEach(function(done){
        client.flushall(done);
    });

    describe('#new Job()', function(){
        var job = jobs[0];

        it('protocol', function(){
            assert.equal(job.protocol, 'universal');
        });
        it('data', function(){
            assert.equal(job.data.name, 'fireque');
        });
        it('protectKey', function(){
            assert.equal(job.protectKey, 'unrestricted');
        });
        it('priority', function(){
            assert.equal(job.priority, 'med');
        });
        it('_getPrefix', function(){
            assert.equal(job._getPrefix(), 'fireque:noname');
        });
        it('_getPrefixforProtocol', function(){
            assert.equal(job._getPrefixforProtocol(), 'fireque:noname:universal');
        });
    });

    describe('#Private Function', function () {
        var job = jobs[0];
        it('_setJob', function (done) {
            jobs[0]._setJob(function (err) {
                assert.equal(err, null);
                model.getJob.bind(obj)(job.uuid, function (err, job_model) {
                    assert.equal(err, null);
                    assert.equal(job_model.protectKey, job.protectKey);
                    assert.equal(job_model.protocol, job.protocol);
                    assert.equal(job_model.priority, job.priority);
                    assert.equal(job_model.schedule, job.schedule);
                    assert.equal(job_model.data.name, 'fireque');
                    assert.equal(job_model.data.num, 0);
                    client.ttl(obj._getPrefix() + ':job:' + job.uuid, function (err, reply) {
                        assert.equal(err, null);
                        assert.equal(reply > 0, true);
                        done();
                    });
                });
            });
        });
        it('_parseOption', function () {
            job._parseOption({
                protectKey: 'key',
                priority: 'low',
                schedule: time
            });
            assert.equal(job.protectKey, 'key');
            assert.equal(job.priority, 'low');
            assert.equal(job.schedule, timestamp);
            job._parseOption({
                protectKey: 'pass',
                priority: 1,
                schedule: 20
            });
            assert.equal(job.protectKey, 'pass');
            assert.equal(job.priority, 'high');
            assert.equal(job.schedule >= timestamp + 20, true);
        });
    });

    describe('#Enqueue Job', function(){

        beforeEach(function(done){
            for (var i = 0; i < jobs.length; i++) {
                jobs[i]._parseOption({ protectKey: 'unrestricted'});
            };
            done();
        });

        it('enqueueTop', function (done) {
            async.series([
                jobs[0].enqueueTop.bind(jobs[0]),
                jobs[1].enqueueTop.bind(jobs[1]),
                function (cb) {
                    jobs[2].enqueueTop({
                        protectKey: 'pass',
                        priority: 1,
                        schedule: 20
                    }, cb);
                }
            ], function (err) {
                assert.equal(err, null);
                async.eachSeries([jobs[2], jobs[1], jobs[0]], function (item, cb) {
                    model.popFromQueue.bind(obj)(function (err, uuid) {
                        assert.equal(err, null);
                        assert.equal(uuid, item.uuid);
                        if ( item.uuid === jobs[2].uuid ) {
                            model.getJob.bind(obj)(item.uuid, function (err, job_model) {
                                assert.equal(err, null);
                                assert.equal(item.protectKey, 'pass');
                                assert.equal(item.priority, 'high');
                                assert.equal(item.schedule >= timestamp + 20, true);
                                cb(err);
                            });
                        }else{
                            cb(err);
                        }
                    });
                }, function (err) {
                    assert.equal(err, null);
                    done();
                });
            });
        });
        it('enqueueAt', function (done) {
            async.eachSeries(jobs, function (item, cb) {
                if (item.data.num > 5) {
                    item.enqueueAt(100, cb);
                }else if (item.data.num == 1) {
                    item.enqueueAt(time, {
                        protectKey: 'pass',
                        priority: 1
                    }, cb);
                }else{
                    item.enqueueAt(time, cb);
                }
            }, function (err) {
                assert.equal(err, null);
                model.fetchScheduleByTimestamp.bind(obj)(timestamp + 50, function (err, reply) {
                    assert.equal(reply[0], timestamp);
                    async.mapSeries([0, 1, 2, 3, 4, 5], function (item, cb) {
                        model.popUuidFromSchedule.bind(obj)(timestamp, cb);
                    }, function (err, result) {
                        for (var i = 0; i < 6; i++) {
                            assert.equal(jobs[i].uuid, result[i]);
                        };
                        model.getJob.bind(obj)(jobs[1].uuid, function (err, job_model) {
                            assert.equal(err, null);
                            assert.equal(job_model.protectKey, 'pass');
                            assert.equal(job_model.priority, 'high');
                            assert.equal(job_model.schedule, timestamp);
                            model.fetchScheduleByTimestamp.bind(obj)(timestamp + 200, function (err, reply) {
                                assert.equal(err, null);
                                assert.equal(reply.length > 0, true);
                                done();
                            });
                        });
                    });
                });
            });
        });
        it('enqueue', function (done) {
            async.eachSeries(jobs, function (item, cb) {
                if (item.data.num > 7) {
                    item.enqueue(1, cb);
                }else if (item.data.num > 5) {
                    item.enqueue('high', cb);
                }else if (item.data.num > 3) {
                    item.enqueue('low', cb);
                }else if (item.data.num == 0) {
                    item.enqueue(time, {
                        protectKey: 'pass',
                        schedule: time
                    }, cb);
                }else{
                    item.enqueue(cb);
                }
            }, function (err) {
                assert.equal(err, null);
                async.eachSeries([jobs[6], jobs[7], jobs[8], jobs[9], jobs[1], jobs[2], jobs[3], jobs[4], jobs[5]], function (item, cb) {
                    model.popFromQueue.bind(obj)(function (err, uuid, from) {
                        if (err == null) {
                            assert.equal(uuid, item.uuid, item.data.num + ' the uuid is error');
                        }
                        cb(err);
                    });
                }, function (err) {
                    assert.equal(err, null);
                    model.popUuidFromSchedule.bind(obj)(timestamp, function (err, reply) {
                        assert.equal(err, null);
                        assert.equal(reply, jobs[0].uuid);
                        done();
                    });
                });
            });
        });
    });

    describe('#Dequeue Job', function(){
        var queue = [
                ':queue',
                ':completed',
                ':failed',
                ':buffer:' + obj.protectKey + ':high',
                ':buffer:' + obj.protectKey + ':med',
                ':buffer:' + obj.protectKey + ':low',
                ':schedule:' + jobs[6].schedule,
                ':processing'
            ];

        it('dequeue', function(done){
            async.series([
                function (cb) {
                    async.each(jobs, function (item, cb) {
                        if ( item.data.num < 8 ) {
                            client.lpush( obj._getPrefixforProtocol() + queue[item.data.num], item.uuid, cb);
                        }else{
                            model.setJob.bind(obj)(item.uuid, item.data, function (err) {
                                if ( err == null ) {
                                    model.setTimeoutOfJob.bind(obj)(item.uuid, 30, cb);
                                }else{
                                    cb(err);
                                }
                            });
                        }
                    }, cb);
                },
                function (cb) {
                    async.each(jobs, function (item, cb) {
                        if ( item.data.num < 8 ) {
                            client.exists( obj._getPrefixforProtocol() + queue[item.data.num], function (err, reply) {
                                assert.equal(reply, 1);
                                cb(err);
                            });
                        }else if ( item.data.num < 9 ){
                            client.exists( obj._getPrefix() + ':job:' + item.uuid, function (err, reply) {
                                assert.equal(reply, 1);
                                cb(err);
                            });
                        }else{
                            client.exists( obj._getPrefix() + ':job:' + item.uuid + ':timeout', function (err, reply) {
                                assert.equal(reply, 1);
                                cb(err);
                            });
                        }
                    }, cb);
                },
                function (cb) {
                    async.each(jobs, function (item, cb) {
                        if ( item.data.num == 7 ) {
                            item.dequeue(function (err, count) {
                                assert.equal(err, 'job is processing');
                                cb(null);
                            });
                        }else{
                            item.dequeue(cb);
                        }
                    }, cb);
                },
                function (cb) {
                    async.each(jobs, function (item, cb) {
                        if ( item.data.num < 7 ) {
                            client.exists( obj._getPrefixforProtocol() + queue[item.data.num], function (err, reply) {
                                assert.equal(reply, 0, obj._getPrefixforProtocol() + queue[item.data.num] + ' should not exists');
                                cb(err);
                            });
                        }else if ( item.data.num < 8 ){
                            client.exists( obj._getPrefixforProtocol() + queue[item.data.num], function (err, reply) {
                                assert.equal(reply, 1);
                                cb(err);
                            });
                        }else if ( item.data.num < 9 ){
                            client.exists( obj._getPrefix() + ':job:' + item.uuid, function (err, reply) {
                                assert.equal(reply, 0, obj._getPrefix() + ':job:' + item.uuid + ' should not exists');
                                cb(err);
                            });
                        }else{
                            client.exists( obj._getPrefix() + ':job:' + item.uuid + ':timeout', function (err, reply) {
                                assert.equal(reply, 0, obj._getPrefix() + ':job:' + item.uuid +  ':timeout' + ' should not exists');
                                cb(err);
                            });
                        }
                    }, cb);
                }
            ], function (err) {
                assert.equal(err, null);
                done();
            });
        });
    });

    describe('#Requeue Job', function(){
        beforeEach(function(done){
            for (var i = 0; i < jobs.length; i++) {
                jobs[i]._parseOption({ protectKey: 'unrestricted'});
            };
            async.each(jobs, function (item, cb) {
                switch (item.data.num) {
                    case 0: item.enqueueTop(cb); break;
                    case 1: item.enqueueTop(cb); break;
                    case 2: item.enqueueAt(100, cb); break;
                    case 3: item.enqueueAt(time, cb); break;
                    case 4: item.enqueue('high', cb); break;
                    case 5: item.enqueue('med', cb); break;
                    case 6: item.enqueue('low', {schedule: 100}, cb); break;
                    case 7: item.enqueue(1, cb); break;
                    case 8: item.enqueue(-1, cb); break;
                    case 9: model.pushToProcessing.bind(obj)(item.uuid, cb); break;
                }
            }, function (err) {
                assert.equal(err, null);
                done();
            })
        });

        it('requeueTop', function (done) {
            async.eachSeries(jobs, function (item, cb) {
                item.requeueTop(cb);
            }, function (err) {
                assert.equal(err, 'job is processing');
                async.parallel([
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':schedule:*', cb);
                    },
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':buffer:*', cb);
                    }
                ], function (err, result) {
                    assert.equal(err, null);
                    for (var i = 0; i < result.length; i++) {
                        assert.equal(result[i].length, 0);
                    };
                    client.lrange( obj._getPrefixforProtocol() + ':queue', -100, 100, function (err, reply) {
                        assert.equal(err, null);
                        jobs.forEach( function (item) {
                            assert.equal(getListCount(reply, item.uuid), item.data.num == 9 ? 0 : 1);
                        });
                        done();
                    });
                });
            });
        });
        it('requeueAt', function (done) {
            async.eachSeries(jobs, function (item, cb) {
                item.requeueAt(time, cb);
            }, function (err) {
                assert.equal(err, 'job is processing');
                async.parallel([
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':queue:*', cb);
                    },
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':buffer:*', cb);
                    }
                ], function (err, result) {
                    assert.equal(err, null);
                    for (var i = 0; i < result.length; i++) {
                        assert.equal(result[i].length, 0);
                    };
                    client.lrange( obj._getPrefixforProtocol() + ':schedule:' + timestamp, -100, 100, function (err, reply) {
                        assert.equal(err, null);
                        jobs.forEach( function (item) {
                            assert.equal(getListCount(reply, item.uuid), item.data.num == 9 ? 0 : 1);
                        });
                        done();
                    });
                });
            });
        });
        it('requeue', function (done) {
            async.eachSeries(jobs, function (item, cb) {
                item.requeue('high', cb);
            }, function (err) {
                assert.equal(err, 'job is processing');
                async.parallel([
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':queue:*', cb);
                    },
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':schedule:*', cb);
                    }
                ], function (err, result) {
                    assert.equal(err, null);
                    for (var i = 0; i < result.length; i++) {
                        assert.equal(result[i].length, 0);
                    };
                    client.lrange( obj._getPrefixforProtocol() + ':buffer:' + obj.protectKey + ':high', -100, 100, function (err, reply) {
                        assert.equal(err, null);
                        jobs.forEach( function (item) {
                            assert.equal(getListCount(reply, item.uuid), item.data.num == 9 ? 0 : 1);
                        });
                        done();
                    });
                });
            });
        });
    });

    describe('#Operate Job', function(){

        beforeEach(function(done){
            for (var i = 0; i < jobs.length; i++) {
                jobs[i]._parseOption({ protectKey: 'unrestricted'});
            };
            async.each(jobs, function (item, cb) {
                switch (item.data.num) {
                    case 0: item.enqueueTop(cb); break;
                    case 1: item.enqueueTop(cb); break;
                    case 2: item.enqueueAt(100, cb); break;
                    case 3: item.enqueueAt(time, cb); break;
                    case 4: item.enqueue('high', cb); break;
                    case 5: item.enqueue('med', cb); break;
                    case 6: item.enqueue('low', {schedule: 100}, cb); break;
                    case 7: item.enqueue(1, cb); break;
                    case 8: item.enqueue(-1, cb); break;
                    case 9: model.pushToProcessing.bind(obj)(item.uuid, cb); break;
                }
            }, function (err) {
                assert.equal(err, null);
                done();
            })
        });

        it('toCompleted', function(done){
            async.each(jobs, function (item, cb) {
                item.toCompleted(cb);
            }, function (err) {
                assert.equal(err, null);
                async.parallel([
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':queue:*', cb);
                    },
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':schedule:*', cb);
                    },
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':buffer:*', cb);
                    }
                ], function (err, result) {
                    assert.equal(err, null);
                    for (var i = 0; i < result.length; i++) {
                        assert.equal(result[i].length, 0);
                    };
                    client.lrange( obj._getPrefixforProtocol() + ':completed', -100, 100, function (err, reply) {
                        assert.equal(err, null);
                        jobs.forEach( function (item) {
                            assert.equal(getListCount(reply, item.uuid), 1);
                        });
                        done();
                    });
                });
            });
        });

        it('toFailed', function(done){
            async.each(jobs, function (item, cb) {
                item.toFailed(cb);
            }, function (err) {
                assert.equal(err, null);
                async.parallel([
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':queue:*', cb);
                    },
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':schedule:*', cb);
                    },
                    function (cb) {
                        client.keys( obj._getPrefixforProtocol() + ':buffer:*', cb);
                    }
                ], function (err, result) {
                    assert.equal(err, null);
                    for (var i = 0; i < result.length; i++) {
                        assert.equal(result[i].length, 0);
                    };
                    client.lrange( obj._getPrefixforProtocol() + ':failed', -100, 100, function (err, reply) {
                        assert.equal(err, null);
                        jobs.forEach( function (item) {
                            assert.equal(getListCount(reply, item.uuid), 1);
                        });
                        done();
                    });
                });
            });
        });
    });
});

