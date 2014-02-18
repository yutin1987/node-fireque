var uuid = require('node-uuid');
var redis = require("redis");

module.exports = (function () {

    var constructor = function (protocol, option) {
        this.protocol = (protocol && protocol.toString()) || 'universal';

        this.workload = (option && option.workload) || this.workload;
        this.timeout = (option && option.timeout) || this.timeout;

        this.timeout = new Date().getTime() + this.timeout * 1000;

        this._wait = (option && option.wait) || this._wait;

        this._connection = (option && option.connection) || redis.createClient(
            (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
            (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
        );

        return this;
    }

    constructor.prototype = {
        protocol: 'universal',
        workload: 100,
        timeout: 30 * 60,
        _wait: 2,
        _connection: null,
        _serviceId: null,
        _worker: null,
        _doListenQueue: false,
        _getPrefix: function () {
            return Fireque._getQueueName() + ':' + this.protocol;
        },
        _popJobFromQueue: function (cb) {
            this._connection.rpoplpush( this._getPrefix() + ':queue', this._getPrefix() + ':processing', function(err, uuid){
                if ( err === null && uuid ) {
                    new Fireque.Job( uuid, function(err, job){
                        cb(err, job);
                        delete job;
                    }, {
                        connection: this._connection
                    });
                }else{
                    cb(err, false);
                }
            }.bind(this));
        },
        _assignJobToWorker: function (job, worker, cb) {
            try{
                worker(job, function (job_err) {
                    if ( job_err || job_err === false ) {
                        job[job_err === false ? 'toCompleted' : 'toFailed'](function(err){
                            cb(err || job_err || null, job);
                        });
                    }else{
                        cb(null, job);
                    }
                });
            }catch(e){
                job.toFailed(function (err) {
                    cb(e.message || e || err, job);
                });
            }
        },
        _listenQueue: function (cb) {
            var worker = this._worker;
            if ( typeof worker === 'function' ){
                async.waterfall([
                    this._popJobFromQueue.bind(this),
                    function (job, cb) {
                        if ( job ) {
                            this._assignJobToWorker(job, worker, cb);
                        }else{
                            cb(null, job);
                        }
                    }.bind(this)
                ], function (err, result) {
                    cb(err, result);
                    
                    delete result;
                    delete worker;
                }.bind(this));
            }else{
                cb('must on perform');
            }
        },
        onPerform: function (worker) {
            this._worker = worker;
            if ( this._serviceId === null ) {
                this._serviceId = setinterval(function(){
                    if ( this._listenQueue === false ) {
                        this._doListenQueue = true;
                        this._listenQueue(function(err, job){
                            if ( err === null ) {
                                console.log('COMPLETED> ', job.uuid);
                            }else{
                                console.log('FAILED> ', err, job && job.uuid);
                            }
                            this._doListenQueue = false;
                        }.bind(this));
                    }
                }.bind(this), this._wait * 1000);
            }
        },
        offPerform: function () {
            clearInterval(this._serviceId);
            this._serviceId = null;
            process.nextTick(function () {
                while ( this._doListenQueue === true);
                cb();
            }.bind(this));
            this._worker = null;
        },
    }

    return constructor;

})();