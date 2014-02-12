var assert = require("assert"),
    Fireque = require("../index.js");


describe('Custom', function(){
    describe('#new Job(null, {}, {port: 9000, host: 10.10.10.10})', function(){
        var job_dev = new Fireque.Job(null, '', {'port': '9000', 'host': '10.10.10.10'});
        it('post should return 9000', function(){
          assert.equal('9000', job_dev._connection.port);
        })
        it('host should return 10.10.10.10', function(){
          assert.equal('10.10.10.10', job_dev._connection.host);
        })
    });

    describe('#new Work(\'push\', {...})', function(){
        var work_dev = new Fireque.Worker('push', {
          workload: 500,
          wait: 60,
          priority: ['high', 'high'],
          host: '192.168.1.1',
          port: 9000
        });
        it('protocol should return universal', function(){
          assert.equal('push', work_dev.protocol);
        });
        it('workload should return 500', function(){
          assert.equal(500, work_dev.workload);
        });
        it('wait should return 60', function(){
          assert.equal(60, work_dev._wait);
        });
        it('priority.length should return 2', function(){
          assert.equal(2, work_dev._priority.length);
        });
        it('port should return 9000', function(){
          assert.equal(9000, work_dev._connection.port);
        });
        it('host should return 192.168.1.1', function(){
          assert.equal('192.168.1.1', work_dev._connection.host);
        });
    });
});