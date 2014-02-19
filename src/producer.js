var uuid = require('node-uuid');
var redis = require("redis");
    async = require("async");


module.exports = (function () {

    var jobs = {};

    var constructor = function (protocol, option) {
        this.protocol = (protocol && (typeof protocol === 'object' && protocol.length ? protocol : [protocol.toString()])) || this.protocol;
        
        this._max_wait = (option && option._max_wait) || this._max_wait;
        this._max_count = (option && option._max_count) || this._max_count;

        this._completed_jobs = [];
        this._failed_jobs = [];

        this._connection = (option && option.connection) || redis.createClient(
            (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
            (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
        );

        return this;
    }

    constructor.prototype = {
        protocol: ['universal'],
        _max_wait: 30,
        _max_count: 10,
        _connection: null,
        _completed_perform: null,
        _completed_jobs: [],
        _completed_max_count: 0,
        _completed_max_wait: 0,
        _completed_timeout: 0,
        _completed_service: null,
        _doListenCompleted: false,
        _failed_perform: null,
        _failed_jobs: [],
        _failed_max_count: 0,
        _failed_max_wait: 0,
        _failed_timeout: 0,
        _failed_service: null,
        _doListenFailed: false,
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
            var jobs = this['_' + status + '_jobs'],
                max_count = this['_' + status + '_max_count'],
                max_wait = this['_' + status + '_max_wait'],
                timeout = this['_' + status + '_timeout'];
            if ( jobs.length >= max_count || new Date().getTime() > timeout ) {
                this['_' + status + '_jobs'] = [];
                this['_' + status + '_timeout'] = new Date().getTime() + max_wait * 1000;

                async.map(jobs, function (uuid, cb){
                    new Fireque.Job(uuid, function(err, job){
                        cb(null, job);
                    }, {
                        connection: this._connection
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
            var perform = this._completed_perform;
            if ( typeof perform === 'function' ) {
                async.series([
                    function (cb) {
                        this._popJobFromQueueByStatus('completed', function (err, uuid) {
                            if ( uuid != false ) {
                                this._completed_jobs.push(uuid);
                            }
                            cb(err);
                        }.bind(this));
                    }.bind(this),
                    function (cb) {
                        this._assignJobToPerform('completed', perform, function (err) {
                            cb(err);
                        });
                    }.bind(this)
                ], cb);
            }else{
                cb('must on completed');
            }
        },
        _listenFailed: function (cb) {
            var perform = this._failed_perform;
            if ( typeof perform === 'function' ) {
                async.series([
                    function (cb) {
                        this._popJobFromQueueByStatus('failed', function (err, uuid) {
                            if ( uuid != false ) {
                                this._failed_jobs.push(uuid);
                            }
                            cb(err);
                        }.bind(this));
                    }.bind(this),
                    function (cb) {
                        this._assignJobToPerform('failed', perform, function (err) {
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
            this._completed_perform = process;
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
            process.nextTick(function () {
                while ( this._doListenCompleted === true);
                this._assignJobToPerform('completed', this._completed_perform, cb);
            }.bind(this));
        },
        onFailed: function (process, option){

            this._failed_max_count = (option && option.max_count) || this.max_count;
            this._failed_max_wait = (option && option.max_wait) || this.max_wait;
            this._failed_perform = process;
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
            process.nextTick(function () {
                while ( this._doListenFailed === true);
                this._assignJobToPerform('failed', this._failed_perform, cb);
            }.bind(this));
        },
        onTimeout: function (process, timeout) {
            var self = this,
                id = this._event_failed.push(process),
                doListen, doAssign;

            this._event_timeout.push(process);

            (doAssign = function(list){
                var ready = list.length,
                    overtime_jobs = [];

                for (var i = 0, length = list.length; i < length; i += 1) {
                    overtime_jobs.push(new Fireque.Job(list[i], function(){
                        ready -= 1;
                        if ( ready < 1 ) {
                            process(overtime_jobs);
                        }
                    }, {
                        connection: self._connection
                    }));
                };
            });

            (doFetch = function(protocol, callBack){
                self._connection.lrange(Fireque._getQueueName() + ':' + protocol + ':processing', -100, 100, function(err, reply){
                    var overtime = new Date().getTime() - ( timeout * 1000 ),
                        list = [],
                        uuid;

                    if ( err === null && reply ) {
                        for (var i = 0, length = reply.length; i < length; i+=1) {
                            uuid = reply[i];
                            if ( jobs[uuid] ) {
                                if ( jobs[uuid] < overtime ) {
                                    list.push(uuid);
                                }
                            }else{
                                jobs[uuid] = new Date().getTime();
                            }
                        }
                    }
                    callBack(list);
                });
            });

            (doListen = function(){
                var overtime_uuid = [],
                    ready = self.protocol.length;

                for (var i = 0, length = ready; i < length; i += 1) {
                    doFetch(self.protocol[i], function(list){
                        overtime_uuid = overtime_uuid.concat(list);
                        ready -= 1;
                        if ( ready < 1 ) {
                            if ( overtime_uuid.length > 0 ) {
                                doAssign(overtime_uuid);
                            }
                            if ( self._event_timeout.indexOf(process) > -1 ){
                                setTimeout(doListen, self._wait * 1000);
                            }
                        }
                    });
                };
            })();
        },
        offTimeout: function(process){
            if ( process === undefined ) {
                while(this._event_timeout.length > 0){
                    this._event_timeout.pop();
                }
            }else{
                var index = this._event_timeout.indexOf(process);
                if ( index > -1 ) {
                    this._event_timeout.splice(index, 1);
                }
            }
        },

    }

    return constructor;

})();