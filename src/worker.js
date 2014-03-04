var uuid = require('node-uuid'),
    async = require('async'),
<<<<<<< HEAD
    redis = require("redis");

module.exports = (function () {

    var constructor = function (protocol, option) {
=======
    model = require("../lib/model.js");

module.exports = (function () {

    var constructor = function (protocol, option, fireSelf) {
        fireSelf._apply(this, option);

        this._fireSelf = fireSelf;

>>>>>>> develop_0.5
        this.protocol = (protocol && protocol.toString()) || 'universal';

        this.workload = (option && option.workload) || this.workload;
        this.workinghour = (option && option.workinghour) || this.workinghour;

        this.timeout = (option && option.timeout) || this.timeout;

        this.priority = (option && option.priority) || this.priority;

<<<<<<< HEAD
        this._wait = (option && option.wait) || this._wait;

        this._connection = (option && option.connection) || redis.createClient(
            (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
            (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
        );

=======
>>>>>>> develop_0.5
        return this;
    }

    constructor.prototype = {
        protocol: 'universal',
        workload: 100,
        workinghour: 30 * 60,
        timeout: 60,
        priority: ['high','high','high','med','med','low'],
<<<<<<< HEAD
        _priority: [],
        _wait: 2,
        _connection: null,
=======
        _fireSelf: null,
        _priority: [],
>>>>>>> develop_0.5
        _serviceId: null,
        _worker: null,
        _handler_work_out: null,
        _handler_affter_perform: null,
        _doListenQueue: false,
<<<<<<< HEAD
        _getPrefix: function () {
            return Fireque._getQueueName();
        },
        _getPrefixforProtocol: function () {
            return Fireque._getQueueName() + ':' + this.protocol;
        },
=======
>>>>>>> develop_0.5
        _delPriority: function (priority, cb) {
            var index = this._priority.indexOf(priority);
            if ( index > -1 ) {
                this._priority.splice(index,1);
<<<<<<< HEAD
=======
            }else{
                this._priority = this.priority.concat();
>>>>>>> develop_0.5
            }
            cb && cb();
        },
        _popJobFromQueue: function (cb) {
<<<<<<< HEAD
            var key = [];
            key.push(this._getPrefixforProtocol() + ':queue');

            this._priority = (this._priority.length > 0 && this._priority) || this.priority.concat();
            for (var i = 0, length = this._priority.length; i < length; i++) {
                key.push( this._getPrefixforProtocol() + ':buffer:unrestricted:' + this._priority[i]);
            };
            key.push( this._getPrefixforProtocol() + ':buffer:unrestricted:high');
            key.push( this._getPrefixforProtocol() + ':buffer:unrestricted:med');
            key.push( this._getPrefixforProtocol() + ':buffer:unrestricted:low');

            key.push(this._wait);

            this._connection.brpop( key , function (err, reply){
                if ( err === null && reply && reply[1] ) {
                    this._delPriority(reply[0].split(':')[5]);
                    new Fireque.Job( reply[1], function(err, job){
                        cb(err, job);
                        delete job;
                    }, {
                        connection: this._connection
=======
            this._priority = (this._priority.length > 0 && this._priority) || this.priority.concat();

            model.popFromQueue.bind(this)(this._priority, function (err, uuid, from) {
                if ( err === null && uuid ) {
                    if ( from ) {
                        this._delPriority(from);
                    }
                    new this._fireSelf.Job(uuid, function (err, job) {
                        cb(err, job);
                        delete job;
>>>>>>> develop_0.5
                    });
                }else{
                    cb(true);
                }
            }.bind(this));
        },
<<<<<<< HEAD
        _pushJobToProcessing: function (uuid, cb) {
            this._connection.lpush( this._getPrefixforProtocol() + ':processing', uuid, cb);
        },
        _setTimeoutOfJob: function (job, cb) {
            async.series([
                function (cb) {
                    this._connection.set( this._getPrefix() + ':job:' + job.uuid + ':timeout', 1, cb);
                }.bind(this),
                function (cb) {
                    this._connection.expire( this._getPrefix() + ':job:' + job.uuid + ':timeout', this.timeout, cb);
                }.bind(this)
            ], function (err) {
                cb && cb(err, job);
            });
        },
=======
>>>>>>> develop_0.5
        _assignJobToWorker: function (job, worker, cb) {
            this.workload -= 1;
            try{
                worker(job, function (job_err) {
                    if ( job_err || job_err === false ) {
                        job[job_err === false ? 'toCompleted' : 'toFailed'](function(err){
<<<<<<< HEAD
                            cb(err || job_err || null, job);
=======
                            cb(err || job_err, job);
>>>>>>> develop_0.5
                        });
                    }else{
                        cb(null, job);
                    }
                });
            }catch(e){
                job.toFailed(function (err) {
<<<<<<< HEAD
                    cb(e.message || e || err, job);
=======
                    cb(err || e.message || e, job);
>>>>>>> develop_0.5
                });
            }
        },
        _listenQueue: function (cb) {
            var worker = this._worker;
            if ( typeof worker === 'function' ){
<<<<<<< HEAD
                async.waterfall([
                    this._popJobFromQueue.bind(this),
                    function (job, cb) {
                        this._pushJobToProcessing(job.uuid, function (err) {
                            cb(err, job);
                        });
                    }.bind(this),
                    function (job, cb) {
                        this._setTimeoutOfJob(job);
                        this._assignJobToWorker(job, worker, cb);
                    }.bind(this)
                ], function (err, result) {
                    cb(err, result);
                    
                    delete result;
                    delete worker;
=======
                this._popJobFromQueue( function (err, job) {
                    if ( err == null && job ) {
                        async.series([
                            function (cb) {
                                model.setTimeoutOfJob.bind(this)(job.uuid, this.timeout, function (err) {
                                    cb(err, job);
                                    delete job;
                                });
                            }.bind(this),
                            function (cb) {
                                model.pushToProcessing.bind(this)(job.uuid, function (err) {
                                    cb(err, job);
                                    delete job;
                                });
                            }.bind(this),
                            function (cb) {
                                this._assignJobToWorker(job, worker, function (err) {
                                    cb(err, job);
                                    delete job;
                                });
                            }.bind(this)
                        ], function (err, result) {
                            if (err != null && result.length < 3) {
                                model.pushToQueue.bind(this)(uuid);
                            }else{
                                model.decrementWorkload.bind(this)(job.protectKey);
                            }
                            cb(err, job);
                        }.bind(this));
                    }else{
                        cb(err);
                    }
>>>>>>> develop_0.5
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
<<<<<<< HEAD
                        if ( this.workload > 0 && this.workinghour > new Date().getTime() ) {
=======
                        if ( this.workload > 0 && this.workinghour > Date.now() ) {
>>>>>>> develop_0.5
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