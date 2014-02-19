var assert = require("assert"),
    async = require("async"),
    Fireque = require("../index.js"),
    redis = require("redis"),
    client = redis.createClient();

describe('Producer', function(){
    beforeEach(function(done){
        client.flushall(done);
    });
  
    describe('#new Producer()', function(){
        var producer = new Fireque.Producer();

        it('protocol should return universal', function(){
            assert.equal(producer.protocol[0], 'universal');
        });
        it('max_count should return 10', function(){
            assert.equal(producer._max_count, 10);
        });
        it('max_wait should return 30', function(){
            assert.equal(producer._max_wait, 30);
        });
        it('_getPrefix should return fireque:noname:universal', function(){
            assert.equal(producer._getPrefix()[0], 'fireque:noname:universal');
        });
    });

    describe('#Producer Private Function', function(){
        var producer = new Fireque.Producer('push'), jobs = [];

        for (var i = 0; i < 10; i+=1) {
            jobs.push(new Fireque.Job('push'));
        };

        this.timeout(5000);

        it('_popJobFromQueueByStatus(`completed`) should get 10 uuid', function (done){
            async.eachSeries(jobs, function (item, cb) {
                item.toCompleted(cb);
            }, function (err) {
                assert.equal(err, null);
                async.eachSeries(jobs, function (item, cb) {
                    producer._popJobFromQueueByStatus('completed', function (err, uuid) {
                        assert.equal(item.uuid, uuid);
                        cb(err);
                    });
                }, function (err, result) {
                    assert.equal(err, null);
                    done();
                    // producer._popJobFromQueueByStatus('completed', function (err, uuid) {
                    //     assert.equal(uuid, false);
                    // });
                });
            });
        });

        it('_popJobFromQueueByStatus(`failed`) should get 10 uuid', function (done){
            async.eachSeries(jobs, function (item, cb) {
                item.toFailed(cb);
            }, function (err) {
                assert.equal(err, null);
                async.eachSeries(jobs, function (item, cb) {
                    producer._popJobFromQueueByStatus('failed', function (err, uuid) {
                        assert.equal(item.uuid, uuid);
                        cb(err);
                    });
                }, function (err, result) {
                    assert.equal(err, null);
                    done();
                    // producer._popJobFromQueueByStatus('failed', function (err, uuid) {
                    //     assert.equal(uuid, false);
                    // });
                });
            });
        });

        it('_assignJobToPerform(`completed`) should get 10 jobs when over 10', function (done) {
            producer._completed_max_count = 10;
            producer._completed_timeout = new Date().getTime() + 60 * 1000;

            async.each(jobs, function (job, cb) {
                producer._completed_jobs.push(job.uuid);
                job.enqueue(cb);
            }, function (err) {
                assert.equal(err, null);
                producer._assignJobToPerform('completed', function (jobs, cb) {
                    cb(null, jobs);
                }, function (err, jobs) {
                    assert.equal(err, null);
                    assert.equal(jobs.length, 10);
                    done();
                });
            });
        });

        it('_assignJobToPerform(`completed`) should get 10 jobs when timeout', function (done) {
            producer._completed_max_count = 100;
            producer._completed_timeout = new Date().getTime() - 10;

            async.each(jobs, function (job, cb) {
                producer._completed_jobs.push(job.uuid);
                job.enqueue(cb);
            }, function (err) {
                assert.equal(err, null);
                producer._assignJobToPerform('completed', function (jobs, cb) {
                    cb(null, jobs);
                }, function (err, jobs) {
                    assert.equal(err, null);
                    assert.equal(jobs.length, 10);
                    done();
                });
            });
        });

        it('_assignJobToPerform(`completed`) should get 0 jobs', function (done) {
            producer._completed_max_count = 100;
            producer._completed_timeout = new Date().getTime() + 60 * 1000;

            producer._assignJobToPerform('completed', function (jobs, cb) {
                cb(null, jobs);
            }, function (err, jobs) {
                assert.equal(err, null);
                assert.equal(jobs, null);
                done();
            });
        });

        it('_assignJobToPerform(`failed`) should get 10 jobs when over 10', function (done) {
            producer._failed_max_count = 10;
            producer._failed_timeout = new Date().getTime() + 60 * 1000;

            async.each(jobs, function (job, cb) {
                producer._failed_jobs.push(job.uuid);
                job.enqueue(cb);
            }, function (err) {
                assert.equal(err, null);
                producer._assignJobToPerform('failed', function (jobs, cb) {
                    cb(null, jobs);
                }, function (err, jobs) {
                    assert.equal(err, null);
                    assert.equal(jobs.length, 10);
                    done();
                });
            });
        });

        it('_assignJobToPerform(`failed`) should get 10 jobs when timeout', function (done) {
            producer._failed_max_count = 100;
            producer._failed_timeout = new Date().getTime() - 10;

            async.each(jobs, function (job, cb) {
                producer._failed_jobs.push(job.uuid);
                job.enqueue(cb);
            }, function (err) {
                assert.equal(err, null);
                producer._assignJobToPerform('failed', function (jobs, cb) {
                    cb(null, jobs);
                }, function (err, jobs) {
                    assert.equal(err, null);
                    assert.equal(jobs.length, 10);
                    done();
                });
            });
        });

        it('_assignJobToPerform(`failed`) should get 0 jobs', function (done) {
            producer._failed_max_count = 100;
            producer._failed_timeout = new Date().getTime() + 60 * 1000;

            producer._assignJobToPerform('failed', function (jobs, cb) {
                cb(null, jobs);
            }, function (err, jobs) {
                assert.equal(err, null);
                assert.equal(jobs, null);
                done();
            });
        });

        it('_listenCompleted should get 1 jobs', function (done) {
            producer._completed_max_count = 0;
            producer._completed_timeout = 0;
            producer._completed_jobs = [];
            producer._completed_perform = function (job, cb) {
                assert.equal(job[0].uuid, jobs[0].uuid);
                cb(null);
            }
            jobs[0].toCompleted(function (err) {
                assert.equal(err, null);
                producer._listenCompleted( function (err) {
                    assert.equal(err, null);
                    done();
                });
            });
        });

        it('_listenFailed should get 1 jobs', function (done) {
            producer._failed_max_count = 0;
            producer._failed_timeout = 0;
            producer._failed_jobs = [];
            producer._failed_perform = function (job, cb) {
                assert.equal(job[0].uuid, jobs[0].uuid);
                cb(null);
            }
            jobs[0].toFailed(function (err) {
                assert.equal(err, null);
                producer._listenFailed( function (err) {
                    assert.equal(err, null);
                    done();
                });
            });
        });
    });


    describe('#Producer on/off', function(){
        var producer = new Fireque.Producer('push'), jobs = [];

        for (var i = 0; i < 10; i+=1) {
            jobs.push(new Fireque.Job('push'));
        };

        it('onCompleted should get 10 jobs', function (done) {
            producer.onCompleted( function (job, cb) {
                assert.equal(job.length, 10);
                for (var i = 0; i < jobs.length; i++) {
                    assert.equal(job[i].uuid, jobs[i].uuid);
                };
                producer._completed_perform = function (job, cb) {
                    assert.equal(job.length, 0);
                    cb(null);
                }
                producer.offCompleted(done);
                cb(null);
            }, {max_count: 10, max_wait: 30});

            async.eachSeries(jobs, function (item, cb) {
                item.toCompleted(cb);
            });
        });

        it('onFailed should get 10 jobs', function (done) {
            producer.onFailed( function (job, cb) {
                assert.equal(job.length, 10);
                for (var i = 0; i < jobs.length; i++) {
                    assert.equal(job[i].uuid, jobs[i].uuid);
                };
                producer._failed_perform = function (job, cb) {
                    assert.equal(job.length, 0);
                    cb(null);
                }
                producer.offFailed(done);
                cb(null);
            }, {max_count: 10, max_wait: 30});

            async.eachSeries(jobs, function (item, cb) {
                item.toFailed(cb);
            });
        });
    });
});