var assert = require("assert"),
    async = require("async"),
    getListCount = require("./getListCount.js"),
    Fireque = require("../index.js"),
    redis = require("redis"),
    client = redis.createClient();

describe('Job', function(){

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
                    });
                });
            });
        });
    });
});

