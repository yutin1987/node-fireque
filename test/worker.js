var assert = require("assert"),
    async = require("async"),
    getListCount = require("./getListCount.js"),
    Fireque = require("../index.js"),
    redis = require("redis"),
    client = redis.createClient();

describe('Worker', function(){
    var worker;
    beforeEach(function(done){
        worker = new Fireque.Worker();
        client.flushall(done);
    });
  
    describe('#new Work()', function(){
        it('protocol should return universal', function(){
            assert.equal('universal', worker.protocol);
        });
        it('workload should return 100', function(){
            assert.equal(100, worker.workload);
        });
        it('wait should return 2', function(){
            assert.equal(2, worker._wait);
        });
        it('timeout should return > now', function(){
            assert.equal(worker.timeout > new Date().getTime(), true);
        });
        it('port should return 6379', function(){
            assert.equal(6379, worker._connection.port);
        });
        it('host should return 127.0.0.1', function(){
            assert.equal('127.0.0.1', worker._connection.host);
        });
        it('_getPrefix should return fireque:noname:universal', function(){
            assert.equal(worker._getPrefix(), 'fireque:noname:universal');
        });
    });

    describe('#Work Private Function', function(){
        var job;
        beforeEach(function(done){
            job = new Fireque.Job(null, {who: "I'm Job."});
            done();
        });

        it('uuid should return null', function(done){
            worker._popJobFromQueue(function (err, job) {
                assert.equal(err, null);
                assert.equal(job, false);
                done();
            });
        });

        it('uuid should return test_uuid', function(done){
            var uuid = 'test_uuid';
            async.parallel([
                function (cb) { 
                    client.hmset( worker._getPrefix() + ':job:' + uuid,
                        'data', JSON.stringify({'justin':'boy'}),
                        'protocol', 'high',
                    cb);
                },
                function(cb){
                    client.lpush( worker._getPrefix() + ':queue', uuid, cb);
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
                client.lrange(worker._getPrefix() + ':completed', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1, 'should in completed and only 1');
                    done();
                });
            });
        });

        it('failed should return uuid when _assignJobToWorker failed', function(done){
            worker._assignJobToWorker(job, function(job, cb) {
                cb(true);
            }, function (err, echo){
                assert.equal(err, true);
                assert.equal(echo.uuid, job.uuid);
                client.lrange(worker._getPrefix() + ':failed', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1, 'should in failed and only 1');
                    done();
                });
            });
        });

        it('failed should return uuid when _assignJobToWorker failed', function(done){
            worker._assignJobToWorker(job, function(job, cb) {
                throw "I'm throw";
            }, function (err, echo){
                assert.equal(err, "I'm throw");
                assert.equal(echo.uuid, job.uuid);
                client.lrange(worker._getPrefix() + ':failed', -100, 100, function(err, reply){
                    assert.equal(getListCount(reply, job.uuid), 1, 'should in failed and only 1');
                    done();
                });
            });
        });
    });

    describe('#Work Perform', function(){
        var job;
        beforeEach(function(done){
            job = new Fireque.Job(null, {who: "I'm Job."});
            done();
        });

        it('_wroker should has work when onPerform', function(done){
            var perform = function () {
                console.log("I'm Perform.");
            };
            worker.onPerform(perform);
            assert.equal(worker._wroker, perform);
            done();
        });

        it('_wroker should not has work when offPerform', function(done){
            var perform = function () {
                console.log("I'm Perform.");
            };
            worker.onPerform(perform);
            assert.equal(worker._wroker, perform);
            worker.offPerform(perform);
            assert.equal(worker._wrokers, null);
            done();
        });

        it('_listenQueue should return job and err is uull', function(done){
            var perform = function (job, cb) {
                job.data = "I'm Perform. and I will Completed.";
                cb(false);
            };
            worker.onPerform(perform);
            job.enqueue(false, function (err) {
                assert.equal(err, null);
                worker._listenQueue( function(err, perform_job) {
                    assert.equal(err, null);
                    assert.equal(perform_job.uuid, job.uuid);
                    assert.equal(perform_job.data, "I'm Perform. and I will Completed.");
                    client.lrange( job._getPrefix() + ':completed', -100, 100, function(err, reply){
                        assert.equal(getListCount(reply, job.uuid), 1);
                        done();
                    });
                });
            });
        });

        it('_listenQueue should return job and err is true', function(done){
            var perform = function (job, cb) {
                job.data = "I'm Perform. and I will Failed.";
                cb(true);
            };
            worker.onPerform(perform);
            job.enqueue(false, function (err) {
                assert.equal(err, null);
                worker._listenQueue( function(err, perform_job) {
                    assert.equal(err, true);
                    assert.equal(perform_job.uuid, job.uuid);
                    assert.equal(perform_job.data, "I'm Perform. and I will Failed.");
                    client.lrange( job._getPrefix() + ':failed', -100, 100, function(err, reply){
                        assert.equal(getListCount(reply, job.uuid), 1);
                        done();
                    });
                });
            });
        });

    });
});