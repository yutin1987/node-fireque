var async = require("async"),
    model = require("../lib/model.js");

module.exports = (function () {

    var jobs = {};

    var constructor = function (protocol, option, fireSelf) {

        fireSelf._apply(this, option);

        this._fireSelf = fireSelf;

        this.protocol = (protocol && protocol.toString()) || 'universal';
        
        this._max_wait = (option && option.max_wait) || this._max_wait;
        this._max_count = (option && option.max_count) || this._max_count;

        this._completed_jobs = [];
        this._failed_jobs = [];
        this._timeout_jobs = [];

        return this;
    }

    constructor.prototype = {
        protocol: 'universal',
        _fireSelf: null,
        _max_wait: 30,
        _max_count: 10,
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
        _assignJobToHandler: function (status, process, cb) {
            var jobs = this['_' + status + '_jobs'],
                max_count = this['_' + status + '_max_count'],
                max_wait = this['_' + status + '_max_wait'],
                timeout = this['_' + status + '_timeout'];
            if ( jobs.length >= max_count || new Date().getTime() > timeout ) {
                this['_' + status + '_jobs'] = [];
                this['_' + status + '_timeout'] = new Date().getTime() + max_wait * 1000;

                async.map(jobs, function (uuid, cb){
                    this._fireSelf.Job(uuid, function(err, job){
                        job.dequeue();
                        cb(null, job);
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
                        model.popFromCompleted.bind(this)( function (err, uuid) {
                            if ( err == null && uuid) {
                                this._completed_jobs.push(uuid);
                            }
                            cb(err);
                        }.bind(this));
                    }.bind(this),
                    function (cb) {
                        this._assignJobToHandler('completed', handler, function (err) {
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
                        model.popFromFailed.bind(this)( function (err, uuid) {
                            if ( err == null && uuid) {
                                this._failed_jobs.push(uuid);
                            }
                            cb(err);
                        }.bind(this));
                    }.bind(this),
                    function (cb) {
                        this._assignJobToHandler('failed', handler, function (err) {
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

            (doCallBack = function (){
                setTimeout(function(){
                    if ( this._doListenCompleted === true ) {
                        doCallBack();
                    }else{
                        this._assignJobToHandler('completed', this._completed_handler, cb);
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
                        this._assignJobToHandler('failed', this._failed_handler, cb);
                    }
                }.bind(this), 200);
            }.bind(this))();
        },
        _filterTimeoutByUuid: function (uuid, cb) {
            async.filter(uuid, function (item, cb) {
                model.getTimeoutOfJob.bind(this)(item, function (err, reply) {
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
        _notifyTimeoutToHandler: function(uuid, handler, cb) {
            async.map(uuid, function (uuid, cb){
                this._fireSelf.Job(uuid, function(err, job){
                    cb(null, job);
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
                    model.fetchFromProcessing.bind(this),
                    this._filterTimeoutByUuid.bind(this),
                    this._filterSurgeForTimeout.bind(this),
                    function (uuid, cb) {
                        if ( uuid && uuid.length > 0 ) {
                            this._notifyTimeoutToHandler(uuid, handler, cb);
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