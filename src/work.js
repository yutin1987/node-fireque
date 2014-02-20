var uuid = require('node-uuid'),
    async = require('async'),
    redis = require("redis");

module.exports = (function () {

    var constructor = function (protocol, option) {
        this.protocol = (protocol && protocol.toString()) || 'universal';

        this.workload = (option && option.workload) || this.workload;
        this.workinghour = (option && option.workinghour) || this.workinghour;

        this.timeout = (option && option.timeout) || this.timeout;

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
        workinghour: 30 * 60,
        timeout: 60,
        _wait: 2,
        _connection: null,
        _serviceId: null,
        _worker: null,
        _handler_work_out: null,
        _handler_affter_perform: null,
        _doListenQueue: false,
        _getPrefix: function () {
            return Fireque._getQueueName() + ':' + this.protocol;
        },
        _popJobFromQueue: function (cb) {
            this._connection.brpoplpush( this._getPrefix() + ':queue', this._getPrefix() + ':processing', this._wait, function(err, uuid){
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
        _setTimeoutOfJob: function (job, cb) {
            async.series([
                function (cb) {
                    this._connection.set( this._getPrefix() + ':timeout:' + job.uuid, 1, cb);
                }.bind(this),
                function (cb) {
                    this._connection.expire( this._getPrefix() + ':timeout:' + job.uuid, this.timeout, cb);
                }.bind(this)
            ], function (err) {
                cb && cb(err, job);
            });
        },
        _assignJobToWorker: function (job, worker, cb) {
            this.workload -= 1;
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
                            this._setTimeoutOfJob(job);
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
        onWorkOut: function (handler) {
            this._handler_work_out = handler;
        },
        onAfterPerform: function (handler) {
            this._handler_affter_perform = handler;
        },
        offAfterPerform: function () {
            this._handler_affter_perform = null;
        },
        onPerform: function (handler) {
            this._worker = handler;
            if ( this.workinghour < 1388419200000 ) {
                this.workinghour = new Date().getTime() + this.workinghour * 1000;
            }
            if ( this._serviceId === null ) {
                this._serviceId = setInterval(function(){
                    if ( this._doListenQueue === false ) {
                        if ( this.workload > 0 && this.workinghour > new Date().getTime() ) {
                            this._doListenQueue = true;
                            this._listenQueue(function(err, job){
                                process.nextTick(function () {
                                    this._handler_affter_perform && this._handler_affter_perform(err, job);
                                }.bind(this));
                                this._doListenQueue = false;
                            }.bind(this));
                        }else{
                            this.offPerform(function () {
                                this._handler_work_out && this._handler_work_out();
                            }.bind(this));
                        }
                    }
                }.bind(this));
            }
        },
        offPerform: function (cb) {
            clearInterval(this._serviceId);
            this._serviceId = null;
            this._worker = null;

            (doCallBack = function (){
                setTimeout(function(){
                    if ( this._doListenQueue === true ) {
                        doCallBack();
                    }else{
                        cb && cb();
                    }
                }.bind(this), 200);
            }.bind(this))();
        },
    }

    return constructor;

})();