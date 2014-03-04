<<<<<<< HEAD
var redis = require("redis"),
    async = require("async");
=======
var async = require("async"),
    model = require("../lib/model.js");
>>>>>>> develop_0.5

module.exports = (function () {

    var jobs = {};

<<<<<<< HEAD
    var constructor = function (protocol, option) {
        this.protocol = (protocol && (typeof protocol === 'object' && protocol.length ? protocol : [protocol.toString()])) || this.protocol;
=======
    var constructor = function (protocol, option, fireSelf) {

        fireSelf._apply(this, option);

        this._fireSelf = fireSelf;

        this.protocol = (protocol && protocol.toString()) || 'universal';
>>>>>>> develop_0.5
        
        this._max_wait = (option && option._max_wait) || this._max_wait;
        this._max_count = (option && option._max_count) || this._max_count;

        this._completed_jobs = [];
        this._failed_jobs = [];
        this._timeout_jobs = [];
<<<<<<< HEAD

        this._connection = (option && option.connection) || redis.createClient(
            (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
            (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
        );
=======
>>>>>>> develop_0.5

        return this;
    }

    constructor.prototype = {
<<<<<<< HEAD
        protocol: ['universal'],
        _max_wait: 30,
        _max_count: 10,
        _connection: null,
=======
        protocol: 'universal',
        _fireSelf: null,
        _max_wait: 30,
        _max_count: 10,
>>>>>>> develop_0.5
        _completed_handler: null,
        _completed_jobs: [],
        _completed_max_count: 0,
        _completed_max_wait: 0,
        _completed_timeout: 0,
        _completed_service: null,
        _doListenCompleted: false,
        _failed_handler: null,
        _failed_jobs: [],
        _failed_max_count: 0,
        _failed_max_wait: 0,
        _failed_timeout: 0,
        _failed_service: null,
        _doListenFailed: false,
        _timeout_service: null,
        _timeout_handler: null,
        _timeout_jobs: [],
        _doListenTimeout: false,
<<<<<<< HEAD
        _getPrefix: function(prefix){
            var keys = [];
            for (var i = 0, length = this.protocol.length; i < length; i += 1) {
                keys.push(Fireque._getQueueName() + ':' + this.protocol[i] + ( prefix && ':' + prefix || '' ) );
            };

            return keys;
        },
        _popJobFromQueueByStatus: function (status, cb) {
            this._connection.brpop( this._getPrefix(status).concat(1), function(err, reply) {
                if ( err === null && reply && reply[1] ) {
                    cb(err, reply[1]);
                }else{
                    cb(err, false);
                }
            });
        },
        _assignJobToPerform: function (status, process, cb) {
=======
        _assignJobToHandler: function (status, process, cb) {
>>>>>>> develop_0.5
            var jobs = this['_' + status + '_jobs'],
                max_count = this['_' + status + '_max_count'],
                max_wait = this['_' + status + '_max_wait'],
                timeout = this['_' + status + '_timeout'];
            if ( jobs.length >= max_count || new Date().getTime() > timeout ) {
                this['_' + status + '_jobs'] = [];
                this['_' + status + '_timeout'] = new Date().getTime() + max_wait * 1000;

                async.map(jobs, function (uuid, cb){
<<<<<<< HEAD
                    new Fireque.Job(uuid, function(err, job){
                        cb(null, job);
                    }, {
                        connection: this._connection
=======
                    this._fireSelf.Job(uuid, function(err, job){
                        cb(null, job);
>>>>>>> develop_0.5
                    });
                }.bind(this) , function (err, result) {
                    process(result, cb);

                    delete jobs;
                    delete result;
                    delete process;
                });
            }else{
                cb(null, null);
            }

            delete max_count;
            delete max_wait;
            delete timeout;
        },
        _listenCompleted: function (cb) {
            var handler = this._completed_handler;
            if ( typeof handler === 'function' ) {
                async.series([
                    function (cb) {
<<<<<<< HEAD
                        this._popJobFromQueueByStatus('completed', function (err, uuid) {
                            if ( uuid != false ) {
=======
                        model.popFromCompleted.bind(this)( function (err, uuid) {
                            if ( err == null && uuid) {
>>>>>>> develop_0.5
                                this._completed_jobs.push(uuid);
                            }
                            cb(err);
                        }.bind(this));
                    }.bind(this),
                    function (cb) {
<<<<<<< HEAD
                        this._assignJobToPerform('completed', handler, function (err) {
=======
                        this._assignJobToHandler('completed', handler, function (err) {
>>>>>>> develop_0.5
                            cb(err);
                        });
                    }.bind(this)
                ], cb);
            }else{
                cb('must on completed');
            }
        },
        _listenFailed: function (cb) {
            var handler = this._failed_handler;
            if ( typeof handler === 'function' ) {
                async.series([
                    function (cb) {
<<<<<<< HEAD
                        this._popJobFromQueueByStatus('failed', function (err, uuid) {
                            if ( uuid != false ) {
=======
                        model.popFromFailed.bind(this)( function (err, uuid) {
                            if ( err == null && uuid) {
>>>>>>> develop_0.5
                                this._failed_jobs.push(uuid);
                            }
                            cb(err);
                        }.bind(this));
                    }.bind(this),
                    function (cb) {
<<<<<<< HEAD
                        this._assignJobToPerform('failed', handler, function (err) {
=======
                        this._assignJobToHandler('failed', handler, function (err) {
>>>>>>> develop_0.5
                            cb(err);
                        });
                    }.bind(this)
                ], cb);
            }else{
                cb('must on failed');
            }
        },
        onCompleted: function (process, option){

            this._completed_max_count = (option && option.max_count) || this.max_count;
            this._completed_max_wait = (option && option.max_wait) || this.max_wait;
            this._completed_handler = process;
            this._completed_timeout = new Date().getTime() + this._completed_max_wait * 1000;
<<<<<<< HEAD

            if ( this._completed_service === null ) {
                this._completed_service = setInterval( function(){
                    if ( this._doListenCompleted === false ) {
                        this._doListenCompleted = true;
                        this._listenCompleted(function(err){
                            if ( err !== null ) {
                                console.log('Err from completed > ', err);
                            }
                            this._doListenCompleted = false;
                        }.bind(this));
                    }
                }.bind(this));
            }
        },
        offCompleted: function(cb){
            clearInterval(this._completed_service);
            this._completed_service = null;
            this._completed_max_count = 0;
            this._completed_timeout = 0;

=======

            if ( this._completed_service === null ) {
                this._completed_service = setInterval( function(){
                    if ( this._doListenCompleted === false ) {
                        this._doListenCompleted = true;
                        this._listenCompleted(function(err){
                            if ( err !== null ) {
                                console.log('Err from completed > ', err);
                            }
                            this._doListenCompleted = false;
                        }.bind(this));
                    }
                }.bind(this));
            }
        },
        offCompleted: function(cb){
            clearInterval(this._completed_service);
            this._completed_service = null;
            this._completed_max_count = 0;
            this._completed_timeout = 0;

>>>>>>> develop_0.5
            (doCallBack = function (){
                setTimeout(function(){
                    if ( this._doListenCompleted === true ) {
                        doCallBack();
                    }else{
<<<<<<< HEAD
                        this._assignJobToPerform('completed', this._completed_handler, cb);
=======
                        this._assignJobToHandler('completed', this._completed_handler, cb);
>>>>>>> develop_0.5
                    }
                }.bind(this), 200);
            }.bind(this))();
        },
        onFailed: function (process, option){

            this._failed_max_count = (option && option.max_count) || this.max_count;
            this._failed_max_wait = (option && option.max_wait) || this.max_wait;
            this._failed_handler = process;
            this._failed_timeout = new Date().getTime() + this._failed_max_wait * 1000;

            if ( this._failed_service === null ) {
                this._failed_service = setInterval( function(){
                    if ( this._doListenFailed === false ) {
                        this._doListenFailed = true;
                        this._listenFailed(function(err){
                            if ( err !== null ) {
                                console.log('Err from failed > ', err);
                            }
                            this._doListenFailed = false;
                        }.bind(this));
                    }
                }.bind(this));
            }
        },
        offFailed: function(cb){
            clearInterval(this._failed_service);
            this._failed_service = null;
            this._failed_max_count = 0;
            this._failed_timeout = 0;

            (doCallBack = function (){
                setTimeout(function(){
                    if ( this._doListenFailed === true ) {
                        doCallBack();
                    }else{
<<<<<<< HEAD
                        this._assignJobToPerform('failed', this._failed_handler, cb);
=======
                        this._assignJobToHandler('failed', this._failed_handler, cb);
>>>>>>> develop_0.5
                    }
                }.bind(this), 200);
            }.bind(this))();
        },
<<<<<<< HEAD
        _fetchUuidFromProcessing: function(cb) {
            this._connection.lrange( this._getPrefix() + ':processing', -1000, 1000, cb);
        },
        _filterTimeoutByUuid: function (uuid, cb) {
            async.filter(uuid, function (item, cb) {
                this._connection.ttl(this._getPrefix() + ':timeout:' + item, function (err, reply) {
=======
        _filterTimeoutByUuid: function (uuid, cb) {
            async.filter(uuid, function (item, cb) {
                model.getTimeoutOfJob.bind(this)(item, function (err, reply) {
>>>>>>> develop_0.5
                    cb(err !== null || reply < 1);
                });
            }.bind(this), function(result){
                cb(null, result);
                delete uuid;
            });
        },
        _filterSurgeForTimeout: function (uuid, cb) {
            async.filter(uuid, function (item, cb) {
                cb(this._timeout_jobs.indexOf(item) > -1);
            }.bind(this), function(result){
                this._timeout_jobs = uuid || [];
                cb(null, result);
                delete uuid;
                delete result;
            }.bind(this));
        },
<<<<<<< HEAD
        _notifyTimeoutOfHandler: function(uuid, handler, cb) {
            async.map(uuid, function (uuid, cb){
                new Fireque.Job(uuid, function(err, job){
                    cb(null, job);
                }, {
                    connection: this._connection
=======
        _notifyTimeoutToHandler: function(uuid, handler, cb) {
            async.map(uuid, function (uuid, cb){
                this._fireSelf.Job(uuid, function(err, job){
                    cb(null, job);
>>>>>>> develop_0.5
                });
            }.bind(this) , function (err, result) {
                handler(result, cb);

                delete uuid;
                delete result;
                delete handler;
            });
        },
        _listenTimeout: function (cb) {
            var handler = this._timeout_handler;
            if ( typeof handler === 'function' ) {
                async.waterfall  ([
<<<<<<< HEAD
                    this._fetchUuidFromProcessing.bind(this),
=======
                    model.fetchFromProcessing.bind(this),
>>>>>>> develop_0.5
                    this._filterTimeoutByUuid.bind(this),
                    this._filterSurgeForTimeout.bind(this),
                    function (uuid, cb) {
                        if ( uuid && uuid.length > 0 ) {
<<<<<<< HEAD
                            this._notifyTimeoutOfHandler(uuid, handler, cb);
=======
                            this._notifyTimeoutToHandler(uuid, handler, cb);
>>>>>>> develop_0.5
                        }else{
                            cb(null);
                        }
                    }.bind(this)
                ], function (err) {
                    cb(err);
                    delete handler;
                });
            }else{
                cb('must on timeout');
            }
        },
        onTimeout: function (handler, wait){
            this._timeout_handler = handler;

            if ( this._timeout_service === null ) {
                this._timeout_service = setInterval( function(){
                    if ( this._doListenTimeout === false ) {
                        this._doListenTimeout = true;
                        this._listenTimeout(function(err){
                            if ( err ) {
                                console.log('Err from Timeout > ', err);
                            }
                            this._doListenTimeout = false;
                        }.bind(this));
                    }
                }.bind(this), (wait || this._max_wait) * 1000);
            }
        },
        offTimeout: function(cb){
            clearInterval(this._timeout_service);
            this._timeout_handler = null;
            (doCallBack = function (){
                setTimeout(function(){
                    if ( this._doListenTimeout === true ) {
                        doCallBack();
                    }else{
                        cb && cb();
                    }
                }.bind(this), 200);
            }.bind(this))();
        }

    }

    return constructor;

})();