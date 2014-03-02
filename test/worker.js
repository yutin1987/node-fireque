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
                done();
            });
        });

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
                    done();
                });
            });
        });

        it('_delPriority', function (done) {
            worker._priority = worker.priority.concat();
            var priority = ['high', 'high', 'high', 'med', 'med', 'low'];
            for (var i = 0; i < priority.length; i++) {
                worker._delPriority(priority[i], function (){
                    assert.equal(worker._priority.length, 5 - i);
                });
            };
            done();
        });
    });

    describe('#Work Perform', function(){
        var job;

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
        });

        this.timeout(5000);

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
            });
        });
    });
});