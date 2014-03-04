var assert = require("assert"),
    async = require("async"),
    getListCount = require("./getListCount.js"),
    Fireque = require("../index.js"),
    model = require("../lib/model.js"),
    redis = require("redis"),
    client = redis.createClient();

describe('Job', function(){

<<<<<<< HEAD
    beforeEach(function(done){
        client.flushall(done);
    });

    describe('#new Job()', function(){
        var job;
        beforeEach(function(done){
            job = new Fireque.Job();
            done();
        });

        it('protocol should return universal', function(){
            assert.equal(job.protocol, 'universal');
        });
        it('data should return ""', function(){
            assert.equal(job.data, '');
        });
        it('protectKey should return unrestricted', function(){
            assert.equal(job.protectKey, 'unrestricted');
        });
        it('priority should return unrestricted', function(){
            assert.equal(job.priority, 'med');
        });
        it('_getPrefix should return fireque:noname', function(){
            assert.equal(job._getPrefix(), 'fireque:noname');
        });
        it('_getPrefixforProtocol should return universal', function(){
            assert.equal(job._getPrefixforProtocol(), 'fireque:noname:universal');
        });
    });

    describe('#enqueue Job', function(){
        var job;
        beforeEach(function(done){
            job = new Fireque.Job('push',{
                name: 'fireque'
            });
            done();
        });

        it('redis should has data', function(done){
            job.enqueue(function(err, job){
                assert.equal(err, null);
                async.parallel([
                    function(cb){
                        client.hgetall(job._getPrefix() + ':job:' + job.uuid, function(err, reply){
                            assert.equal(reply.data, JSON.stringify({ name: 'fireque' }));
                            assert.equal(reply.protectKey, 'unrestricted');
                            assert.equal(reply.protocol, 'push');
                            assert.equal(reply.priority, 'med');
                            cb(err);
                        });
                    },
                    function(cb){
                        client.ttl(job._getPrefix() + ':job:' + job.uuid, function(err, reply){
                            assert.equal(reply > 0, true);
                            cb(err);
                        });
                    }
                ], function (err) {
                    assert.equal(err, null);
                    done();
                });
            });
        });

        it('job should in buffer and is med when ``, ``', function(done){
            job.enqueue(function(err, job){
                assert.equal(err, null);
                client.rpop(job._getPrefixforProtocol() + ':buffer:unrestricted:med', function(err, reply){
                    assert.equal(err, null);
                    assert.equal(reply, job.uuid);
                    done();
                });
            });
        });

        it('job should in queue when `true`, ``', function(done){
            job.enqueue(true, function(err, job){
                assert.equal(err, null);
                client.rpop(job._getPrefixforProtocol() + ':queue', function(err, reply){
                    assert.equal(err, null);
                    assert.equal(reply, job.uuid);
                    done();
                });
            });
        });

        it('job should in buffer and is med when `ca`, ``', function(done){
            job.enqueue('ca', function(err, job){
                assert.equal(err, null);
                client.rpop(job._getPrefixforProtocol() + ':buffer:ca:med', function(err, reply){
                    assert.equal(err, null);
                    assert.equal(reply, job.uuid);
                    done();
                });
            });
        });

        it('job should in buffer and is low when ``, `low`', function(done){
            job.enqueue('low', function(err, job){
                assert.equal(err, null);
                client.rpop(job._getPrefixforProtocol() + ':buffer:unrestricted:low', function(err, reply){
                    assert.equal(err, null);
                    assert.equal(reply, job.uuid);
                    done();
                });
            });
        });

        it('job should in buffer and is high when `ca`, `high`', function(done){
            job.enqueue('ca', 'high', function(err, job){
                assert.equal(err, null);
                client.rpop(job._getPrefixforProtocol() + ':buffer:ca:high', function(err, reply){
                    assert.equal(err, null);
                    assert.equal(reply, job.uuid);
                    done();
                });
            });
        });

        it('job should in queue when enqueue(true)', function(done){
            var jobA = new Fireque.Job();
            var jobB = new Fireque.Job();
            jobA.enqueue(true, function(err, job){
                assert.equal(err, null);
                jobB.enqueue(true, function(err, job){
                    assert.equal(err, null);
                    client.lrange(job._getPrefixforProtocol() + ':queue', -100, 100, function(err, reply){
                        assert.equal(err, null);
                        assert.equal(reply[0], jobA.uuid);
                        assert.equal(reply[1], jobB.uuid);
                        new Fireque.Worker()._popJobFromQueue( function (err, job) {
                            assert.equal(err, null);
                            assert.equal(job.uuid, jobB.uuid);
                            done();
                        });
                    });
                });
            });
        });

    });

    describe('#dequeue Job', function(){
        var job = new Fireque.Job(),
            queue = [ ':queue', ':completed', ':failed', ':buffer:ca:high', ':buffer:ca:med', ':buffer:ca:low'],
            key = [ ':job:' + job.uuid, ':job:' + job.uuid + ':timeout'];

        job.protectKey = 'ca';

        it('_delJobByKey after redis should no has data for job', function (done) {
            async.each(key, function (item, cb) {
                client.set( job._getPrefix() + item, job.uuid, cb);
            }, function (err) {
                job._delJobByKey(function (err, reply){
                    assert.equal(err, null);
                    async.map(key, function (item, cb) {
                        client.exists( job._getPrefix() + item, cb);
                    }, function (err, result) {
                        for (var i = result.length - 1; i > -1; i-=1) {
                            assert.equal(result[i], 0);
                        };
                        done();
                    });
                });
            });
        });

        it('_delJobByQueue after job should no has in queue', function (done) {
            async.each(queue, function (item, cb) {
                client.lpush( job._getPrefixforProtocol() + item, job.uuid, cb);
            }, function (err) {
                job._delJobByQueue(function (err, reply){
                    assert.equal(err, null);
                    assert.equal(reply, 6);
                    done();
                });
            });
        });

        it('_checkJobInProcessing should return true when job is processing', function(done){
            client.lpush(job._getPrefixforProtocol() + ':processing', job.uuid, function(err) {
                assert.equal(err, null);
                job._checkJobInProcessing(function (err, bool){
                    assert.equal(err, null);
                    assert.equal(bool, true);
                    done();
                });
            });
        });

        it('_checkJobInProcessing should return false when job is not processing', function(done){
            job._checkJobInProcessing(function (err, bool){
                assert.equal(err, null);
                assert.equal(bool, false);
                done();
            });
        });

        it('dequeue should no job data.', function(done){
            job.enqueue('ca', function(err){
                assert.equal(err, null);
                client.exists(job._getPrefix() + ':job:' + job.uuid, function(err, reply){
                    assert.equal(err, null);
                    assert.equal(reply, 1);
                    job.dequeue(function(){
                        client.exists(job._getPrefix() + ':job:' + job.uuid, function(err, reply){
                            assert.equal(err, null);
                            assert.equal(reply, 0);
                            done();
                        });
                    });
                });
            });
        });

        it('dequeue should no job in the queue.', function(done){
            async.each(queue, function (item, cb) {
                client.lpush(item, job.uuid, cb);
            }, function (err) {
                assert.equal(err, null);
                job.dequeue(function(err){
                    assert.equal(err, null);
                    async.map(queue, function (item, cb) {
                        client.rpop(job._getPrefixforProtocol() + item, function (err, reply){
                            assert.equal(reply, null);
                            cb(err);
                        });
                    }, function(err){
                        assert.equal(err, null);
                        done();
                    });
                });
            });
        });

        it('dequeue should return error when job is processing.', function(done){
            client.lpush(job._getPrefixforProtocol() + ':processing', job.uuid, function(err) {
                assert.equal(err, null);
                job.dequeue(function(err){
                    assert.equal(err, 'job is processing');
                    done();
                });
            });
        });
    });

    describe('#requeue Job', function(){
        var job;
        beforeEach(function(done){
            job = new Fireque.Job();
            done();
        });

        it('job should from high to low', function(done){
            job.enqueue('high', function(err, job){
                assert.equal(err, null);
                client.lrange(job._getPrefixforProtocol() + ':buffer:unrestricted:high', -100, 100, function (err, reply){
                    assert.equal(err, null);
                    assert.equal(getListCount(reply, job.uuid), 1);
                    job.requeue('low', function (err, job){
                        async.map([
                            job._getPrefixforProtocol() + ':buffer:unrestricted:high',
                            job._getPrefixforProtocol() + ':buffer:unrestricted:low'
                        ], function (item, cb) {
                            client.lrange( item, -100, 100, cb);
                        }, function (err, result) {
                            assert.equal(err, null);
                            assert.equal(getListCount(result[0], job.uuid), 0);
                            assert.equal(getListCount(result[1], job.uuid), 1);
                            done();
                        });
                    });
                });
            });
        });

        it('dequeue should return error when job is processing', function(done){
            client.lpush(job._getPrefixforProtocol() + ':processing', job.uuid, function(err) {
                assert.equal(err, null);
                job.requeue(function(err){
                    assert.equal(err, 'job is processing');
                    done();
                });
            });
        });

    });

    describe('#operate Job', function(){
        var job;

        beforeEach(function(done){
            job = new Fireque.Job();
            done();
        });

        it('job should in the completed', function(done){
            job.enqueue(function (err, job) {
                assert.equal(err, null);
                job.data = {name: "I'm completed"};
                job.toCompleted(function(){
                    async.map([
                        job._getPrefixforProtocol() + ':buffer:unrestricted:med',
                        job._getPrefixforProtocol() + ':completed'
                    ], function (item, cb) {
                        client.lrange( item, -100, 100, cb);
                    }, function (err, result) {
                        assert.equal(err, null);
                        assert.equal(getListCount(result[0], job.uuid), 0);
                        assert.equal(getListCount(result[1], job.uuid), 1);
                        new Fireque.Job(job.uuid, function (err, job) {
                            assert.equal(err, null);
                            assert.equal(job.data.name, "I'm completed");
                            done();
                        });
                    });
                });
            });
        });

        it('job should in the failed', function(done){
            job.enqueue('ca', function (err) {
                assert.equal(err, null);
                job.data = {name: "I'm failed"};
                job.toFailed(function(){
                    async.map([
                        job._getPrefixforProtocol() + ':buffer:ca:med',
                        job._getPrefixforProtocol() + ':failed'
                    ], function (item, cb) {
                        client.lrange( item, -100, 100, cb);
                    }, function (err, result) {
                        assert.equal(err, null);
                        assert.equal(getListCount(result[0], job.uuid), 0);
                        assert.equal(getListCount(result[1], job.uuid), 1);
                        new Fireque.Job(job.uuid, function (err, job) {
                            assert.equal(err, null);
                            assert.equal(job.data.name, "I'm failed");
                            done();
                        });
=======
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
>>>>>>> develop_0.5
                    });
                });
            });
        });
    });
});

