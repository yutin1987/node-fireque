var uuid = require('node-uuid'),
    async = require('async'),
    model = require("../lib/model.js");

module.exports = (function () {

    var constructor = function (protocol, option, fireSelf) {
        fireSelf._apply(this, option);

        this._fireSelf = fireSelf;

        this.protocol = (protocol && protocol.toString()) || 'universal';

        this.workload = (option && option.workload) || this.workload;
        this.workinghour = (option && option.workinghour) || this.workinghour;

        this.timeout = (option && option.timeout) || this.timeout;

        this.priority = (option && option.priority) || this.priority;

        this._wait = (option && option.wait) || this._wait;

        return this;
    }

    constructor.prototype = {
        protocol: 'universal',
        workload: 100,
        workinghour: 30 * 60,
        timeout: 60,
        priority: ['high','high','high','med','med','low'],
        _fireSelf: null,
        _priority: [],
        _wait: 2,
        _serviceId: null,
        _worker: null,
        _handler_work_out: null,
        _handler_affter_perform: null,
        _doListenQueue: false,
        _delPriority: function (priority, cb) {
            var index = this._priority.indexOf(priority);
            if ( index > -1 ) {
                this._priority.splice(index,1);
            }
            cb && cb();
        },
        _popJobFromQueue: function (cb) {
            this._priority = (this._priority.length > 0 && this._priority) || this.priority.concat();

            model.popFromQueue.bind(this)(this._priority, function (err, uuid, from) {
                if ( err === null && uuid ) {
                    if ( from ) {
                        this._delPriority(from);
                    }
                    new this._fireSelf.Job(uuid, function (err, job) {
                        cb(err, job);
                        delete job;
                    });
                }else{
                    cb(true);
                }
            }.bind(this));
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
                        model.pushToProcessing.bind(this)(job.uuid, function (err) {
                            cb(err, job);
                        });
                    }.bind(this),
                    function (job, cb) {
                        model.setTimeoutOfJob.bind(this)(job.uuid);
                        this._assignJobToWorker(job, worker, cb);
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