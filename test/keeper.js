var assert = require("assert"),
    async = require("async"),
    Fireque = require("../index.js"),
    redis = require("redis"),
    getListCount = require("./getListCount.js"),
    client = redis.createClient(),
    model = require("../lib/model.js");

describe('Keeper', function(){

    var obj = Fireque._apply({});
    obj.protectKey = 'unrestricted';
    obj.protocol = 'universal';

    var groups = ['GROUP_1', 'GROUP_2', 'GROUP_3', 'GROUP_4', 'GROUP_5'];

    var jobs = [];
    for (var i = 0; i < 5; i++) {
        jobs.push(new Fireque.Job(null,
            {name: 'fireque', num: i}
        ));
    };

    var keeper;
    beforeEach(function(done){
        client.flushall(done);
        keeper = new Fireque.Keeper();
    });

    describe('#Private Function', function(){

        beforeEach(function(done){
            async.each(jobs, function (item, cb) {
                item.enqueue({protectKey: groups[item.data.num]}, cb);
            }, function (err) {
                assert.equal(err, null);
                done();
            });
        });

        it('_fetchPriorityByProtect', function (done) {
            for (var i = 0; i < groups.length; i+=1) {
                keeper._priority[groups[i]] = keeper._fetchPriorityByProtect(groups[i]);
                assert.equal(keeper._priority[groups[i]].length, 6);
            };
            for (var i = 0; i < groups.length; i+=1) {
                keeper._priority[groups[i]].pop();
            };
            for (var i = 0; i < groups.length; i+=1) {
                assert.equal(keeper._fetchPriorityByProtect(groups[i]).length, 5);
            };
            done();
        });
        it('_fetchLowWorklandForProtect', function (done) {
            keeper._fetchLowWorklandForProtect( function (err, protectKey) {
                assert.equal(err, null);
                for (var i = 0; i < 5; i+=1) {
                    assert.equal(groups.indexOf(protectKey[i]) > -1, true);
                };
                async.each([0,1], function (item, cb) {
                    async.each([1,2,3,4,5], function (i, cb) {
                        keeper._getLicenseByProtect(groups[item], cb);
                    }, cb);
                }, function (err) {
                    assert.equal(err, null);
                    keeper._fetchLowWorklandForProtect( function (err, protectKey) {
                        assert.equal(protectKey.length, 3);
                        for (var i = 2; i < 5; i+=1) {
                            assert.equal(groups.indexOf(protectKey[i-2]) > -1, true);
                        };
                        done();
                    });
                });
            });
        });
    });

    describe('#Liccense', function(){
        it('_getLicenseByProtect', function (done) {
            async.eachSeries([1,2,3,4,5,6], function (item, cb) {
                async.each(groups, function (item, cb) {
                    keeper._getLicenseByProtect(item, cb);
                }, function (err) {
                    assert.equal(err, item == 6 ? true : null);
                    cb(err);
                });
            }, function (err) {
                assert.equal(err, true);
                done();
            });
        });
        it('_returnLiccenseByProtect', function (done) {
            async.eachSeries([1,2,3,4,5,6], function (item, cb) {
                async.each(groups, function (item, cb) {
                    keeper._getLicenseByProtect(item, cb);
                }, function (err) {
                    assert.equal(err, item == 6 ? true : null);
                    cb(err);
                });
            }, function (err) {
                assert.equal(err, true);
                async.eachSeries([1,2,3], function (index, cb) {
                    async.each(groups, function (item, cb) {
                        if (index == 1) {
                            keeper._returnLiccenseByProtect(item, cb);
                        }else if (index == 2) {
                            keeper._getLicenseByProtect(item, cb);
                        }else if (index == 3) {
                            keeper._getLicenseByProtect(item, function (err) {
                                assert.equal(err, true);
                                cb(null);
                            });
                        }
                    }, function (err) {
                        cb(err);
                    });
                }, function (err) {
                    assert.equal(err, null);
                    done();
                });
            });
        });
    });

    describe('#Listen Buffer', function(){

        var priority = ['high', 'low', 'med', 'med', 'med'];

        beforeEach(function(done){
            async.eachSeries(jobs, function (item, cb) {
                item.enqueue(priority[item.data.num], {protectKey: 'keeper'}, cb);
            }, function (err) {
                assert.equal(err, null);
                done();
            });
        });

        it('_popBufferToQueueByProtect', function (done) {
            async.mapSeries(jobs, function (item, cb) {
                keeper._popBufferToQueueByProtect('keeper', [], cb);
            }, function (err, result) {
                [0,2,3,4,1].forEach(function (index, i) {
                    assert.equal(result[i].uuid, jobs[index].uuid);
                    assert.equal(result[i].from, jobs[index].priority);
                });
                client.lrange( obj._getPrefixforProtocol() + ':queue', -100, 100, function (err, reply) {
                    assert.equal(err, null);
                    assert.equal(reply.length, 5);
                    jobs.forEach( function (item) {
                        assert.equal(getListCount(reply, item.uuid), 1);
                    });
                    done();
                });
            });
        });
        it('_listenBuffer', function (done) {
            keeper.workload = 4;
            keeper._listenBuffer(function (err, result) {
                assert.equal(result.high, 1);
                assert.equal(result.med, 2);
                assert.equal(result.low, 1);
                client.lrange( obj._getPrefixforProtocol() + ':queue', -100, 100, function (err, reply) {
                    assert.equal(err, null);
                    assert.equal(reply.length, 4);
                    [0,1,2,3].forEach( function (item) {
                        assert.equal(getListCount(reply, jobs[item].uuid), 1);
                    });
                    done();
                });
            });
        });
    });

    describe('#Listen Schedule', function(){

        var priority = ['high', 'low', 'med', 'med', 'med'];

        var now = Date.now();

        var timeOld = new Date(now - 200 * 1000);
        var timeNew = new Date(now + 200 * 1000);

        var timestamp = parseInt(now / 1000);

        beforeEach(function(done){
            async.eachSeries(jobs, function (item, cb) {
                item.enqueueAt(item.data.num < 2 ? timeOld : timeNew, {protectKey: 'keeper', priority: priority[item.data.num]}, cb);
            }, function (err) {
                assert.equal(err, null);
                done();
            });
        });

        it('_popScheduleToBuffer', function (done) {
            async.mapSeries(jobs, function (item, cb) {
                if ( item.data.num < 2 ){
                    keeper._popScheduleToBuffer(timestamp - 200, cb);
                }else{
                    keeper._popScheduleToBuffer(timestamp + 200, cb);
                }
            }, function (err, result) {
                jobs.forEach(function (job, i) {
                    assert.equal(result[i], job.uuid);
                });
                async.parallel([
                    function (cb) {
                        client.lrange( obj._getPrefixforProtocol() + ':buffer:keeper:med', -100, 100, function (err, reply) {
                            if (err == null) {
                                assert.equal(reply.length, 3);
                                [2,3,4].forEach( function (i) {
                                    assert.equal(getListCount(reply, jobs[i].uuid), 1);
                                });
                            }
                            cb(err);
                        });
                    },
                    function (cb) {
                        client.lrange( obj._getPrefixforProtocol() + ':buffer:keeper:high', -100, 100, function (err, reply) {
                            if (err == null) {
                                assert.equal(reply.length, 1);
                                [0].forEach( function (i) {
                                    assert.equal(getListCount(reply, jobs[i].uuid), 1);
                                });
                            }
                            cb(err);
                        });
                    },
                    function (cb) {
                        client.lrange( obj._getPrefixforProtocol() + ':buffer:keeper:low', -100, 100, function (err, reply) {
                            if (err == null) {
                                assert.equal(reply.length, 1);
                                [1].forEach( function (i) {
                                    assert.equal(getListCount(reply, jobs[i].uuid), 1);
                                });
                            }
                            cb(err);
                        });
                    }
                ], function (err) {
                    assert.equal(err, null);
                    done();
                });
            });
        });
        it('_listenSchedule', function (done) {
            keeper._listenSchedule(function (err, total) {
                assert.equal(err, null);
                assert.equal(total[timestamp - 200], 2);
                done();
            });
        });
    });

    describe('#Service', function(){

        var priority = ['high', 'low', 'med', 'med', 'med'];

        var time = new Date();
        var timestamp = parseInt(time / 1000);

        beforeEach(function(done){
            async.eachSeries(jobs, function (item, cb) {
                item.enqueueAt(time, {protectKey: 'keeper', priority: priority[item.data.num]}, cb);
            }, function (err) {
                assert.equal(err, null);
                done();
            });
        });

        it('start', function (done) {
            var count = 0;
            keeper.workload = 4;
            keeper.start(function (err, reply) {
                count += 1;
                switch (count) {
                    case 1:
                        assert.equal(reply['schedule'][timestamp], 5);
                        break;
                    case 2:
                        assert.equal(reply['buffer']['high'], 1);
                        assert.equal(reply['buffer']['med'], 2);
                        assert.equal(reply['buffer']['low'], 1);
                        break;
                    default:
                        keeper.stop();
                        done();
                }
            }, 0);
        });
    });
});