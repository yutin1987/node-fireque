var assert = require("assert"),
    async = require("async"),
    Fireque = require("../index.js"),
    getListCount = require("./getListCount.js"),
    model = require("../lib/model.js");
    redis = require("redis"),
    client = redis.createClient();

<<<<<<< HEAD
describe('Producer', function(){

    var producer = new Fireque.Producer('push'), jobs = [];
=======
describe('Consumer', function(){

    var consumer = new Fireque.Consumer('push'), jobs = [];
>>>>>>> develop_0.5

    for (var i = 0; i < 10; i+=1) {
        jobs.push(new Fireque.Job('push',{num: i}));
    };

    beforeEach(function(done){
        client.flushall(done);
    });
  
    describe('#new Consumer()', function(){
        var consumer = new Fireque.Consumer();

        it('protocol', function(){
            assert.equal(consumer.protocol, 'universal');
        });
        it('max_count', function(){
            assert.equal(consumer._max_count, 10);
        });
        it('max_wait', function(){
            assert.equal(consumer._max_wait, 30);
        });
    });

    describe('#Private Function', function(){
        this.timeout(5000);

        it('_assignJobToHandler(`completed`)', function (done) {
            async.mapSeries([
                { max: 10, timeout: Date.now() + 60 * 1000},
                { max: 100, timeout: Date.now() - 60 * 1000},
                { max: 100, timeout: Date.now() + 60 * 1000},
            ], function (item, cb) {
                consumer._completed_max_count = item.max;
                consumer._completed_timeout = item.timeout;
                consumer._completed_jobs = [];
                async.each(jobs, function (job, cb) {
                    consumer._completed_jobs.push(job.uuid);
                    job.enqueue(cb);
                }, function (err) {
                    assert.equal(err, null);
                    consumer._assignJobToHandler('completed', function (jobs, cb) {
                        cb(null, jobs);
                    }, function (err, jobs) {
                        cb(err, jobs);
                    });
                })
            }, function (err, length) {
                assert.equal(err, null);
                assert.equal(length[0].length, 10);
                assert.equal(length[1].length, 10);
                assert.equal(length[2], null);
                done();
            });
        });

        it('_assignJobToHandler(`failed`)', function (done) {
            async.mapSeries([
                { max: 10, timeout: Date.now() + 60 * 1000},
                { max: 100, timeout: Date.now() - 60 * 1000},
                { max: 100, timeout: Date.now() + 60 * 1000},
            ], function (item, cb) {
                consumer._failed_max_count = item.max;
                consumer._failed_timeout = item.timeout;
                consumer._failed_jobs = [];
                async.each(jobs, function (job, cb) {
                    consumer._failed_jobs.push(job.uuid);
                    job.enqueue(cb);
                }, function (err) {
                    assert.equal(err, null);
                    consumer._assignJobToHandler('failed', function (jobs, cb) {
                        cb(null, jobs);
                    }, function (err, jobs) {
                        cb(err, jobs);
                    });
                })
            }, function (err, length) {
                assert.equal(err, null);
                assert.equal(length[0].length, 10);
                assert.equal(length[1].length, 10);
                assert.equal(length[2], null);
                done();
            });
        });

        it('_filterTimeoutByUuid', function (done) {
            async.map(jobs, function (item, cb) {
                if ( item.data.num < 5 ) {
                    model.setTimeoutOfJob.bind(item)(item.uuid, 60, function (err) {
                        assert.equal(err, null);
                        cb(null, item.uuid);
                    });
                }else{
                    cb(null, item.uuid);
                }
            }, function (err, result) {
                assert.equal(err, null);
                consumer._filterTimeoutByUuid(result, function (err, reply) {
                    assert.equal(reply.length, 5);
                    result.forEach(function (uuid, i) {
                        if ( i < 5 ) {
                            assert.equal(getListCount(reply, uuid), 0);
                        }else{
                            assert.equal(getListCount(reply, uuid), 1);
                        }
                    });
                    done();
                });
            });
        });

        it('_filterSurgeForTimeout', function (done) {
            consumer._filterSurgeForTimeout([jobs[0].uuid, jobs[1].uuid, jobs[2].uuid, jobs[3].uuid, jobs[4].uuid], function (err, uuid) {
                assert.equal(err, null);
                assert.equal(uuid.length, 0);
                consumer._filterSurgeForTimeout([jobs[1].uuid, jobs[2].uuid, jobs[3].uuid], function (err, uuid) {
                    assert.equal(err, null);
                    assert.equal(uuid.length, 3);
                    assert.equal(uuid.indexOf(jobs[1].uuid) > -1, true);
                    assert.equal(uuid.indexOf(jobs[2].uuid) > -1, true);
                    assert.equal(uuid.indexOf(jobs[3].uuid) > -1, true);
                    done();
                });
            });
        });

        it('_notifyTimeoutToHandler', function (done) {
            async.each(jobs, function (item, cb) {
                item.enqueue(cb);
            }, function (err, result) {
                assert.equal(err, null);
                consumer._notifyTimeoutToHandler([jobs[0].uuid, jobs[1].uuid, jobs[2].uuid, jobs[3].uuid, jobs[4].uuid], function (timeout_jobs, cb) {
                    assert.equal(jobs.length, 10);
                    for (var i = 0; i < 5; i++) {
                        assert.equal(timeout_jobs[i].data.num, jobs[i].data.num);
                    };
                    cb(null, 'ok');
                }, function (err, result) {
                    assert.equal(err, null);
                    assert.equal(result, 'ok');
                    done();
                });
            });
        });
    });

    describe('#Listen', function () {
        it('_listenCompleted', function (done) {
            async.mapSeries([
                { max: 10, timeout: Date.now() + 60 * 1000},
                { max: 100, timeout: Date.now() - 60 * 1000},
            ], function (item, cb) {
                consumer._completed_max_count = item.max;
                consumer._completed_timeout = item.timeout;
                consumer._completed_jobs = [];
                consumer._completed_handler = function (completed_jobs, cb) {
                    completed_jobs = completed_jobs.map(function(item){
                        return item.uuid;
                    });
                    jobs.forEach(function (job) {
                        assert.equal(completed_jobs.indexOf(job.uuid) > -1, true);
                    });
                    cb(null);
                    done();
                }

                async.each(jobs, function (job, cb) {
                    job.toCompleted(cb);
                }, function (err) {
                    async.eachSeries(jobs, function (item, cb) {
                        consumer._listenCompleted(cb);
                    });
                });
            });
        });

        it('_listenFailed', function (done) {
            async.mapSeries([
                { max: 10, timeout: Date.now() + 60 * 1000},
                { max: 100, timeout: Date.now() - 60 * 1000},
            ], function (item, cb) {
                consumer._failed_max_count = item.max;
                consumer._failed_timeout = item.timeout;
                consumer._failed_jobs = [];
                consumer._failed_handler = function (failed_jobs, cb) {
                    failed_jobs = failed_jobs.map(function(item){
                        return item.uuid;
                    });
                    jobs.forEach(function (job) {
                        assert.equal(failed_jobs.indexOf(job.uuid) > -1, true);
                    });
                    cb(null);
                    done();
                }

                async.each(jobs, function (job, cb) {
                    job.toFailed(cb);
                }, function (err) {
                    async.eachSeries(jobs, function (item, cb) {
                        consumer._listenFailed(cb);
                    });
                });
            });
        });

        it('_listenTimeout', function (done) {
            consumer._timeout_jobs = [];

            consumer._timeout_handler = function (timeout_jobs, cb) {
                timeout_jobs = timeout_jobs.map( function (item) {
                    return item.uuid;
                });
                jobs.forEach(function (job) {
                    assert.equal(timeout_jobs.indexOf(job.uuid) > -1, true);
                });
                cb();
            };

            async.each(jobs, function (item, cb) {
                model.pushToProcessing.bind(item)(item.uuid, cb);
            }, function (err) {
                assert.equal(err, null);
                consumer._listenTimeout(function () {
                    consumer._listenTimeout(function () {
                        done();
                    });
                });
            });
        });
    });


    describe('#Consumer on/off', function(){
        this.timeout(10000);

        it('onCompleted', function (done) {

            async.eachSeries(jobs, function (item, cb) {
                item.toCompleted(cb);
            }, function (err) {
                assert.equal(err, null);
                consumer.onCompleted( function (completed_jobs, cb) {
                    assert.equal(completed_jobs.length, 10);
                    for (var i = 0; i < jobs.length; i++) {
                        assert.equal(completed_jobs[i].uuid, jobs[i].uuid);
                    };
                    consumer._completed_handler = function (job, cb) {
                        assert.equal(job.length, 0);
                        cb(null);
                    }
                    consumer.offCompleted(done);
                    cb(null);
                }, {max_count: 10, max_wait: 30});
            });
        });

        it('onFailed', function (done) {
            async.eachSeries(jobs, function (item, cb) {
                item.toFailed(cb);
            }, function (err) {
                assert.equal(err, null);
                consumer.onFailed( function (failed_jobs, cb) {
                    assert.equal(failed_jobs.length, 10);
                    for (var i = 0; i < jobs.length; i++) {
                        assert.equal(failed_jobs[i].uuid, jobs[i].uuid);
                    };
                    consumer._failed_handler = function (job, cb) {
                        assert.equal(job.length, 0);
                        cb(null);
                    }
                    consumer.offFailed(done);
                    cb(null);
                }, {max_count: 10, max_wait: 30});
            });
        });


        it('onTimeout', function (done) {
            async.each(jobs, function (job, cb) {
                model.pushToProcessing.bind(job)(job.uuid, cb);
            }, function (err) {
                assert.equal(err, null);
                consumer.onTimeout(function (timeout_jobs, cb) {
                    timeout_jobs = timeout_jobs.map( function (item) {
                        return item.uuid;
                    });
                    jobs.forEach(function (job) {
                        assert.equal(timeout_jobs.indexOf(job.uuid) > -1, true);
                    });
                    consumer.offTimeout(done);
                    cb();
                }, 1);
            });
        });
    });
});