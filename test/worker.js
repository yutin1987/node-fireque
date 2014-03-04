var assert = require("assert"),
    async = require("async"),
    getListCount = require("./getListCount.js"),
    Fireque = require("../index.js"),
    model = require("../lib/model.js"),
    redis = require("redis"),
    client = redis.createClient();

describe('Worker', function(){
    var worker;
    beforeEach(function(done){
        worker = new Fireque.Worker();
        client.flushall(done);
<<<<<<< HEAD
    });
  
    describe('#new Work()', function(){
        it('protocol should return universal', function(){
            assert.equal('universal', worker.protocol);
        });
        it('workload should return 100', function(){
            assert.equal(100, worker.workload);
        });
        it('workinghour should return 1800', function(){
            assert.equal(worker.workinghour, 1800);
        });
        it('wait should return 2', function(){
            assert.equal(2, worker._wait);
        });
        it('timeout should return 60', function(){
            assert.equal(60, worker.timeout);
        });
        it('port should return 6379', function(){
            assert.equal(6379, worker._connection.port);
        });
        it('host should return 127.0.0.1', function(){
            assert.equal('127.0.0.1', worker._connection.host);
        });
        it('_getPrefixforProtocol should return fireque:noname:universal', function(){
            assert.equal(worker._getPrefixforProtocol(), 'fireque:noname:universal');
        });
    });

    describe('#Work Private Function', function(){
        var job;
        beforeEach(function(done){
            job = new Fireque.Job(null, {who: "I'm Job."});
            done();
        });

        this.timeout(5000);

        it('uuid should return null', function(done){
            worker._wait = 1;
            worker._popJobFromQueue(function (err, job) {
                assert.equal(err, true);
                assert.equal(worker._priority.length, 6);
=======
    });

    var jobs = [];
    var jobs_priority = ['high','low','med','med','med','high','high','low','low','high'];
    for (var i = 0; i < 10; i++) { 
        jobs.push(new Fireque.Job(null, {who: "I'm Job." + i, num: i}));
    };
  
    describe('#new Work()', function(){
        it('protocol', function(){
            assert.equal('universal', worker.protocol);
        });
        it('workload', function(){
            assert.equal(100, worker.workload);
        });
        it('workinghour', function(){
            assert.equal(worker.workinghour, 1800);
        });
        it('timeout', function(){
            assert.equal(60, worker.timeout);
        });
        it('port', function(){
            assert.equal(6379, worker._connection.port);
        });
        it('host', function(){
            assert.equal('127.0.0.1', worker._connection.host);
        });
        it('_getPrefix', function(){
            assert.equal(worker._getPrefix(), 'fireque:noname');
        });
        it('_getPrefixforProtocol', function(){
            assert.equal(worker._getPrefixforProtocol(), 'fireque:noname:universal');
        });
    });

    describe('#Private Function', function(){
        this.timeout(5000);

        beforeEach(function(done){
            async.each(jobs, function (job, cb) {
                job.enqueue(jobs_priority[job.data.num], cb);
            }, function (err) {
                assert.equal(err, null);
>>>>>>> develop_0.5
                done();
            });
        });

<<<<<<< HEAD
        it('uuid should return test_uuid', function(done){
            var uuid = 'test_uuid';
            async.parallel([
                function (cb) { 
                    client.hmset( worker._getPrefixforProtocol() + ':job:' + uuid,
                        'data', JSON.stringify({'justin':'boy'}),
                        'protocol', 'high',
                    cb);
                },
                function(cb){
                    client.lpush( worker._getPrefixforProtocol() + ':queue', uuid, cb);
                }
            ], function( err, result) {
                assert.equal(err, null);
                worker._popJobFromQueue(function (err, job) {
                    assert.equal(err, null);
                    assert.equal(job.uuid, uuid);
                    done();
                });
            });
        });

        it('completed should return uuid when _assignJobToWorker completed', function(done){
            worker._assignJobToWorker(job, function(job, cb) {
                cb(false);
            }, function (err, echo){
                assert.equal(err, null);
                assert.equal(echo.uuid, job.uuid);
                client.lrange(worker._getPrefixforProtocol() + ':completed', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1, 'should in completed and only 1');
                    done();
                });
            });
        });

        it('failed should return uuid when _assignJobToWorker failed', function (done) {
            worker._assignJobToWorker(job, function(job, cb) {
                cb(true);
            }, function (err, echo){
                assert.equal(err, true);
                assert.equal(echo.uuid, job.uuid);
                client.lrange(worker._getPrefixforProtocol() + ':failed', -100, 100, function (err, reply) {
                    assert.equal(getListCount(reply, job.uuid), 1, 'should in failed and only 1');
                    done();
                });
            });
        });

        it('failed should return uuid when _assignJobToWorker failed', function (done) {
            worker._assignJobToWorker(job, function(job, cb) {
                throw "I'm throw";
            }, function (err, echo){
                assert.equal(err, "I'm throw");
                assert.equal(echo.uuid, job.uuid);
                client.lrange(worker._getPrefixforProtocol() + ':failed', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1, 'should in failed and only 1');
                    done();
                });
            });
        });

        it('TTL should return > 0 when _setTimeoutOfJob', function (done) {
            worker._setTimeoutOfJob(job, function(err, job) {
                assert.equal(err, null);
                client.ttl(worker._getPrefix() + ':job:' + job.uuid + ':timeout', function(err, reply){
                    assert.equal(err, null);
                    assert.equal(reply > 0, true);
                    done();
                });
            });
        });

        it('_pushJobToProcessing after shoud has uuid in processing', function (done) {
            worker._pushJobToProcessing('xyz123', function (err) {
                assert.equal(err, null);
                client.lrange( worker._getPrefixforProtocol() + ':processing', -100, 100, function (err, reply) {
                    assert.equal(err, null);
                    assert.equal(getListCount(reply, 'xyz123'), 1);
=======
        it('_popJobFromQueue', function(done){
            async.eachSeries([0, 5, 6, 2, 3, 1, 9, 4, 7, 8], function (item, cb) {
                worker._popJobFromQueue(function (err, job) {
                    assert.equal(err, null);
                    assert.equal(job.uuid, jobs[item].uuid);
                    assert.equal(job.who, jobs[item].who);
                    cb(null);
                });
            }, function () {
                done();
            });
        });

        it('_assignJobToWorker', function(done){
            async.each(jobs, function (job, cb) {
                worker._assignJobToWorker(job, function(job, cb) {
                    if ( job.data.num < 3 ) {
                        cb(false);
                    }else if ( job.data.num < 6 ) {
                        cb(true);
                    }else if ( job.data.num < 9 ) {
                        throw "I'm throw";
                    }else{
                        cb();
                    }
                }, function (err) {
                    cb();
                });
            }, function (err) {
                assert.equal(err, null);
                async.parallel({
                    completed: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':completed', -100, 100, cb);
                    },
                    failed: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':failed', -100, 100, cb);
                    }
                }, function (err, result) {
                    assert.equal(err, null);
                    for (var i = 0; i < 10; i++) {
                        if ( i < 3 ) {
                            assert.equal(getListCount(result.completed, jobs[i].uuid), 1);
                            assert.equal(getListCount(result.failed, jobs[i].uuid), 0);
                        }else if ( i < 6 ) {
                            assert.equal(getListCount(result.completed, jobs[i].uuid), 0);
                            assert.equal(getListCount(result.failed, jobs[i].uuid), 1);
                        }else if ( i < 9 ) {
                            assert.equal(getListCount(result.completed, jobs[i].uuid), 0);
                            assert.equal(getListCount(result.failed, jobs[i].uuid), 1);
                        }else{
                            assert.equal(getListCount(result.completed, jobs[i].uuid), 0);
                            assert.equal(getListCount(result.failed, jobs[i].uuid), 0);
                        }
                    };
>>>>>>> develop_0.5
                    done();
                });
            });
        });

<<<<<<< HEAD
        it('_delPriority after _priority shoud return 5', function (done) {
            worker._priority = worker.priority.concat();
            worker._delPriority('high', function (){
                assert.equal(worker._priority.length, 5);
                done();
            });
=======
        it('_delPriority', function (done) {
            worker._priority = worker.priority.concat();
            var priority = ['high', 'high', 'high', 'med', 'med', 'low'];
            for (var i = 0; i < priority.length; i++) {
                worker._delPriority(priority[i], function (){
                    assert.equal(worker._priority.length, 5 - i);
                });
            };
            done();
>>>>>>> develop_0.5
        });
    });

    describe('#Work Perform', function(){
        var job;
<<<<<<< HEAD
        beforeEach(function(done){
            job = new Fireque.Job(null, {who: "I'm Job."});
            done();
=======

        beforeEach(function(done){
            async.each(jobs, function (job, cb) {
                async.parallel([
                    function (cb) {
                        if (job.data.num == 9){
                            job.enqueueTop(cb);
                        }else{
                            job.enqueue(jobs_priority[job.data.num], cb);
                        }
                    },
                    function (cb) {
                        model.incrementWorkload.bind(job)(job.protectKey, cb);
                    }
                ], cb);
            }, function (err) {
                assert.equal(err, null);
                done();
            });
>>>>>>> develop_0.5
        });

        this.timeout(5000);

<<<<<<< HEAD
        it('_listenQueue should return job from completed', function(done){
            var perform = function (job, cb) {
                job.data = "I'm Perform. and I will Completed.";
                client.lrange( job._getPrefixforProtocol() + ':processing', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1);
                    cb(false);
                });
            };
            worker._worker = perform;
            job.enqueue(true, function (err) {
                assert.equal(err, null);
                worker._listenQueue( function(err, perform_job) {
                    assert.equal(err, null);
                    assert.equal(perform_job.uuid, job.uuid);
                    assert.equal(perform_job.data, "I'm Perform. and I will Completed.");
                    client.lrange( job._getPrefixforProtocol() + ':completed', -100, 100, function(err, reply){
                        assert.equal(getListCount(reply, job.uuid), 1);
                        done();
                    });
                });
            });
        });

        it('_listenQueue should return job from failed', function(done){
            var perform = function (job, cb) {
                job.data = "I'm Perform. and I will Failed.";
                client.lrange( job._getPrefixforProtocol() + ':processing', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1);
                    cb(true);
                });
            };
            worker._worker = perform;
            job.enqueue(true, function (err) {
                assert.equal(err, null);
                worker._listenQueue( function(err, perform_job) {
                    assert.equal(err, true);
                    assert.equal(perform_job.uuid, job.uuid);
                    assert.equal(perform_job.data, "I'm Perform. and I will Failed.");
                    client.lrange( job._getPrefixforProtocol() + ':failed', -100, 100, function(err, reply){
                        assert.equal(getListCount(reply, job.uuid), 1);
                        done();
                    });
                });
            });
        });

        it('onPerform should completed a job', function (done) {
            worker.onPerform(function (job, cb) {
                job.data = "I'm onPerform";
                assert.equal(worker.workinghour > 1388419200000, true);
                assert.equal(worker.workload > 10, true);
                client.lrange( job._getPrefixforProtocol() + ':processing', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1);
                    worker.offPerform(function(){
                        done();
                    });
                    cb(false);
                });
            });
            job.enqueue(true, function (err) {
                assert.equal(err, null);
            });
        });

        it('onAfterPerform should get a job', function (done) {
            var ready = 2;
            worker.onAfterPerform( function(err, perform_job) {
                assert.equal(err, null);
                assert.equal(perform_job.uuid, job.uuid);
                worker.offAfterPerform();
                ready -= 1;
                ready || done();
            });
            worker.onPerform(function (job, cb) {
                job.data = "I'm onPerform";
                client.lrange( job._getPrefixforProtocol() + ':processing', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1);
                    worker.offPerform(function(){
                        ready -= 1;
                        ready || done();
                    });
                    cb(false);
                });
            });
            job.enqueue(true, function (err) {
                assert.equal(err, null);
            });
        });

        it('onWorkOut should run workload < 1', function (done) {
            var ready = 2;
            worker.workload = 1;
            worker.workinghour = 60;
            worker.onPerform(function (job, cb) {
                job.data = "I'm onPerform";
                client.lrange( job._getPrefixforProtocol() + ':processing', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1);
                    cb(false);
                });
            });
            worker.onWorkOut( function () {
                done();
            });
            job.enqueue(true, function (err) {
                assert.equal(err, null);
            });
        });

        it('onWorkOut should run workinghour < now', function (done) {
            var ready = 2;
            worker.workload = 60;
            worker.workinghour = 1;
            worker.onPerform(function (job, cb) {
                job.data = "I'm onPerform";
                client.lrange( job._getPrefixforProtocol() + ':processing', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1);
                    cb(false);
                });
            });
            worker.onWorkOut( function () {
                done();
            });
            job.enqueue(true, function (err) {
                assert.equal(err, null);
            });
        });

        it('priority should high -> low -> queue -> high', function (done) {
            async.series([
                function (cb) {
                    client.lpush( worker._getPrefixforProtocol() + ':buffer:unrestricted:high' ,'xyz' , cb);
                },
                function (cb) {
                    worker._popJobFromQueue( function () {
                        assert.equal(worker._priority[0], 'high');
                        assert.equal(worker._priority[1], 'high');
                        assert.equal(worker._priority[2], 'med');
                        assert.equal(worker._priority[3], 'med');
                        assert.equal(worker._priority[4], 'low');
                        assert.equal(worker._priority.length, 5);
                        cb();
                    });
                },
                function (cb) {
                    client.lpush( worker._getPrefixforProtocol() + ':buffer:unrestricted:low' ,'xyz' , cb);
                },
                function (cb) {
                    worker._popJobFromQueue( function () {
                        assert.equal(worker._priority[0], 'high');
                        assert.equal(worker._priority[1], 'high');
                        assert.equal(worker._priority[2], 'med');
                        assert.equal(worker._priority[3], 'med');
                        assert.equal(worker._priority.length, 4);
                        cb();
                    });
                },
                function (cb) {
                    client.lpush( worker._getPrefixforProtocol() + ':queue' ,'xyz' , cb);
                },
                function (cb) {
                    client.lpush( worker._getPrefixforProtocol() + ':buffer:unrestricted:high' ,'xyz' , cb);
                },
                function (cb) {
                    worker._popJobFromQueue( function () {
                        assert.equal(worker._priority[0], 'high');
                        assert.equal(worker._priority[1], 'high');
                        assert.equal(worker._priority[2], 'med');
                        assert.equal(worker._priority[3], 'med');
                        assert.equal(worker._priority.length, 4);
                        cb();
                    });
                },
                function (cb) {
                    worker._popJobFromQueue( function () {
                        assert.equal(worker._priority[0], 'high');
                        assert.equal(worker._priority[1], 'med');
                        assert.equal(worker._priority[2], 'med');
                        assert.equal(worker._priority.length, 3);
                        cb();
                    });
                }
            ], function (err) {
                assert.equal(err, null);
                done();
=======
        it('_listenQueue', function(done){
            worker._worker = function (job, cb) {
                job.data.res = "I'm _listenQueue" + job.data.num;
                model.fetchFromProcessing.bind(job)(function (err, reply) {
                    assert.equal(err, null);
                    assert.equal(getListCount(reply, job.uuid), 1);
                    if ( job.data.num < 4 ) {
                        cb(false);
                    }else if ( job.data.num < 8 ) {
                        cb(true);
                    }else{
                        cb();
                    }
                });
            };
            async.eachSeries([9, 0, 5, 6, 2, 3, 1, 4, 7, 8], function (item, cb) {
                worker._listenQueue( function(err, perform_job) {
                    if ( perform_job.data.num < 4 ) {
                        assert.equal(err, null);
                    }else if ( perform_job.data.num < 8 ) {
                        assert.equal(err, true);
                    }else{
                        assert.equal(err, null);
                    }
                    assert.equal(perform_job.uuid, jobs[item].uuid);
                    assert.equal(perform_job.data.res, "I'm _listenQueue" + jobs[item].data.num);
                    cb();
                });
            }, done);
        });

        it('onPerform', function (done) {
            var index = -1, jobs_sort = [9, 0, 5, 6, 2, 3, 1, 4, 7, 8];
            workload = worker.workload;
            assert.equal(workload > 0, true);
            worker.onPerform(function (job, cb) {
                job.data.res = "I'm onPerform" + job.data.num;
                index += 1;
                assert.equal(job.uuid, jobs[jobs_sort[index]].uuid);
                if ( index < 4 ) {
                    cb(false);
                }else if ( index < 8 ) {
                    cb(true);
                }else if ( index < 9 ) {
                    throw "I'm throw";
                }else{
                    worker.offPerform(function () {
                        async.parallel({
                            completed: function (cb) {
                                client.lrange(worker._getPrefixforProtocol() + ':completed', -100, 100, cb);
                            },
                            failed: function (cb) {
                                client.lrange(worker._getPrefixforProtocol() + ':failed', -100, 100, cb);
                            },
                            processing: function (cb) {
                                client.lrange(worker._getPrefixforProtocol() + ':processing', -100, 100, cb);
                            },
                            queue: function (cb) {
                                client.lrange(worker._getPrefixforProtocol() + ':queue', -100, 100, cb);
                            },
                            high: function (cb) {
                                client.lrange(worker._getPrefixforProtocol() + ':buffer:' + jobs[0].protectKey + ':high', -100, 100, cb);
                            },
                            med: function (cb) {
                                client.lrange(worker._getPrefixforProtocol() + ':buffer:' + jobs[0].protectKey + ':med', -100, 100, cb);
                            },
                            low: function (cb) {
                                client.lrange(worker._getPrefixforProtocol() + ':buffer:' + jobs[0].protectKey + ':low', -100, 100, cb);
                            }
                        }, function (err, result) {
                            assert.equal(err, null);
                            assert.equal(result.completed.length, 4);
                            assert.equal(result.failed.length, 5);
                            assert.equal(result.processing.length, 1);
                            assert.equal(result.queue.length, 0);
                            assert.equal(result.high.length, 0);
                            assert.equal(result.med.length, 0);
                            assert.equal(result.low.length, 0);
                            assert.equal(getListCount(result.completed, jobs[9].uuid), 1);
                            assert.equal(getListCount(result.completed, jobs[0].uuid), 1);
                            assert.equal(getListCount(result.completed, jobs[5].uuid), 1);
                            assert.equal(getListCount(result.completed, jobs[6].uuid), 1);
                            assert.equal(getListCount(result.failed, jobs[2].uuid), 1);
                            assert.equal(getListCount(result.failed, jobs[3].uuid), 1);
                            assert.equal(getListCount(result.failed, jobs[1].uuid), 1);
                            assert.equal(getListCount(result.failed, jobs[4].uuid), 1);
                            assert.equal(getListCount(result.failed, jobs[7].uuid), 1);
                            assert.equal(getListCount(result.processing, jobs[8].uuid), 1);
                            done();
                        });
                    }.bind(this));
                    cb();
                }
            });
        });

        it('onAfterPerform', function (done) {
            var index = -1, jobs_sort = [9, 0, 5, 6, 2, 3, 1, 4, 7, 8];
            worker.onAfterPerform( function(err, perform_job) {
                if ( index < 4 ) {
                    assert.equal(err, null);
                }else if ( index < 8 ) {
                    assert.equal(err, true);
                }else if ( index < 9 ) {
                    assert.equal(err, "I'm throw");
                }else{
                    assert.equal(err, null);
                }
            });
            worker.onPerform(function (job, cb) {
                job.data.res = "I'm onAfterPerform" + job.data.num;
                index += 1;
                assert.equal(job.uuid, jobs[jobs_sort[index]].uuid);
                if ( index < 4 ) {
                    cb(false);
                }else if ( index < 8 ) {
                    cb(true);
                }else if ( index < 9 ) {
                    throw "I'm throw";
                }else{
                    worker.offPerform(done);
                    cb();
                }
            });
        });

        it('onWorkOut over workload', function (done) {
            var index = -1;
            worker.workload = 5;
            worker.workinghour = 60;
            worker.onPerform(function (job, cb) {
                job.data = "I'm onWorkOut";
                index += 1;
                if ( index < 2 ) {
                    cb(false);
                }else if ( index < 4 ) {
                    cb(true);
                }else if ( index < 5 ) {
                    throw "I'm throw";
                }else{
                    cb();
                }
            });
            worker.onWorkOut( function () {
                async.parallel({
                    completed: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':completed', -100, 100, cb);
                    },
                    failed: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':failed', -100, 100, cb);
                    },
                    processing: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':processing', -100, 100, cb);
                    },
                    queue: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':queue', -100, 100, cb);
                    },
                    high: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':buffer:' + jobs[0].protectKey + ':high', -100, 100, cb);
                    },
                    med: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':buffer:' + jobs[0].protectKey + ':med', -100, 100, cb);
                    },
                    low: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':buffer:' + jobs[0].protectKey + ':low', -100, 100, cb);
                    }
                }, function (err, result) {
                    assert.equal(result.completed.length, 2);
                    assert.equal(result.failed.length, 3);
                    assert.equal(result.processing.length, 0);
                    assert.equal(result.queue.length, 0);
                    assert.equal(result.high.length + result.med.length + result.low.length, 5);
                    assert.equal(getListCount(result.completed, jobs[9].uuid), 1);
                    assert.equal(getListCount(result.completed, jobs[0].uuid), 1);
                    assert.equal(getListCount(result.failed, jobs[5].uuid), 1);
                    assert.equal(getListCount(result.failed, jobs[6].uuid), 1);
                    assert.equal(getListCount(result.failed, jobs[2].uuid), 1);
                    done();
                });
            });
        });
        it('onWorkOut over workinghour', function (done) {
            var index = -1;
            worker.workload = 100;
            worker.workinghour = 1;
            worker.onPerform(function (job, cb) {
                job.data = "I'm onWorkOut";
                index += 1;
                if ( index < 4 ) {
                    cb(false);
                }else if ( index < 8 ) {
                    cb(true);
                }else if ( index < 9 ) {
                    throw "I'm throw";
                }else{
                    cb();
                }
            });
            worker.onWorkOut( function () {
                async.parallel({
                    completed: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':completed', -100, 100, cb);
                    },
                    failed: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':failed', -100, 100, cb);
                    },
                    processing: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':processing', -100, 100, cb);
                    },
                    queue: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':queue', -100, 100, cb);
                    },
                    high: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':buffer:' + jobs[0].protectKey + ':high', -100, 100, cb);
                    },
                    med: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':buffer:' + jobs[0].protectKey + ':med', -100, 100, cb);
                    },
                    low: function (cb) {
                        client.lrange(worker._getPrefixforProtocol() + ':buffer:' + jobs[0].protectKey + ':low', -100, 100, cb);
                    }
                }, function (err, result) {
                    assert.equal(result.completed.length, 4);
                    assert.equal(result.failed.length, 5);
                    assert.equal(result.processing.length, 1);
                    assert.equal(result.queue.length, 0);
                    // assert.equal(result.high.length + result.med.length + result.low.length, 5);
                    // assert.equal(getListCount(result.completed, jobs[9].uuid), 1);
                    // assert.equal(getListCount(result.completed, jobs[0].uuid), 1);
                    // assert.equal(getListCount(result.failed, jobs[5].uuid), 1);
                    // assert.equal(getListCount(result.failed, jobs[6].uuid), 1);
                    // assert.equal(getListCount(result.failed, jobs[2].uuid), 1);
                    done();
                });
>>>>>>> develop_0.5
            });
        });
    });
});