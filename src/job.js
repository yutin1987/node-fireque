var uuid = require('node-uuid'),
<<<<<<< HEAD
    redis = require("redis"),
    async = require("async");
=======
    async = require("async"),
    util = require("util"),
    model = require("../lib/model.js");
>>>>>>> develop_0.5

module.exports = (function () {

    // protocol, data, option
    // uuid, cb, option
    var constructor = function () {
<<<<<<< HEAD
        var callback = arguments[1] || function(){};

        var option = arguments[2];
        this._connection = (option && option.connection) || redis.createClient(
            (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
            (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
        );
=======
        var cb = arguments[1];
        var option = arguments[2];
        var fireSelf = arguments[3];

        fireSelf._apply(this, option);
>>>>>>> develop_0.5

        if ( typeof arguments[1] !== 'function' ){
            this.uuid = uuid.v4();
            this.protocol = (arguments[0] && arguments[0].toString()) || 'universal';
            this.data = arguments[1] || '';
<<<<<<< HEAD
            this.protectKey = (option && option.protectKey) || this.protectKey;
            this.priority = (option && option.priority) || this.priority;
        }else{
            this.uuid = arguments[0];
            this._connection.hgetall(this._getPrefix() + ':job:' + this.uuid, function(err, reply){
                if ( err === null && reply){
                    for(var key in reply){ 
                        if ( key == 'data' ){
                            this['data'] = JSON.parse(reply[key]);
                        }else{
                            this[key] = reply[key];
                        }
                    }
                }
                callback(err, this);
=======
            this._parseOption(option);
        }else{
            this.uuid = arguments[0];
            model.getJob.bind(this)(this.uuid, function (err, reply) {
                if ( err === null && reply){
                    for(var key in reply){ 
                        this[key] = reply[key];
                    }
                }
                cb(err, this);
>>>>>>> develop_0.5
            }.bind(this));
        }

        return this;
    }

    constructor.prototype = {
        uuid: '',
        protocol: 'universal',
        data: '',
        protectKey: 'unrestricted',
        priority: 'med',
<<<<<<< HEAD
        _connection: null,
        _getPrefix: function(){
            return Fireque._getQueueName();
        },
        _getPrefixforProtocol: function(){
            return Fireque._getQueueName() + ':' + this.protocol;
        },
        _expire: function(){
            this._connection.expire( this._getPrefix() + ':job:' + this.uuid, 3 * 24 * 60 * 60);
        },
        _clean: function(callback) {
            this._connection.lrem(this._getPrefixforProtocol() + ':processing', 0, this.uuid, function(err, reply){
                if ( err === null && reply < 1 ) {
                    this.dequeue(callback);
                }else{
                    callback(err, this);
                }
            }.bind(this));
        },
        enqueue: function(){
            var protectKey, priority, callback, queue, type;

            for (var i = arguments.length - 1; i >= 0; i-=1) {
                type = typeof arguments[i];
                if ( type === 'function' ) {
                    callback = arguments[i];
                }else if ( type === 'boolean') {
                    protectKey = arguments[i];
                }else if ( arguments[i] === 'high' || arguments[i] === 'med' || arguments[i] === 'low' ) {
                    priority = arguments[i];
                }else{
                    protectKey = arguments[i];
                }
            };

            callback = callback || function(){};
            this.priority = priority || this.priority;
            this.protectKey = protectKey || (protectKey === false ? protectKey : this.protectKey);

            this._connection.hmset( this._getPrefix() + ':job:' + this.uuid,
                'data', JSON.stringify(this.data),
                'protectKey', this.protectKey,
                'protocol', this.protocol,
                'priority', this.priority,
            function(err, reply){
                if ( err !== null ) {
                    callback(err, this);
                }else{
                    this._expire();
                    if ( protectKey === true ) {
                        this._connection.rpush( this._getPrefixforProtocol() + ':queue', this.uuid, function(err, reply) {
                            callback(err, this);
                        }.bind(this));
                    }else{
                        this._connection.lpush( this._getPrefixforProtocol() + ':buffer:' + this.protectKey + ':' + this.priority, this.uuid, function(err, reply) {
                            callback(err, this);
                        }.bind(this));
=======
        schedule: 0,
        _setJob: function (cb) {
            async.series([
                function (cb) {
                    model.setJob.bind(this)(this.uuid, {
                        'data': this.data,
                        'protectKey': this.protectKey,
                        'protocol': this.protocol,
                        'priority': this.priority,
                        'schedule': this.schedule
                    }, cb);
                }.bind(this),
                function (cb) {
                    model.expireJob.bind(this)(this.uuid, cb);
                }.bind(this)
            ], function (err) {
                cb(err);
            });
        },
        _parseOption: function (opt) {
            var priority, schedule, grade = ['low', 'med', 'high'];

            opt = opt || {};

            this.protectKey = opt.protectKey || this.protectKey;

            if ( typeof opt.priority === 'number' ) {
                opt.priority = grade[opt.priority + 1];
            }
            if ( grade.indexOf(opt.priority) < 0 ) {
                opt.priority = 'med';
            }
            this.priority = opt.priority || 'med';

            if ( util.isDate(opt.schedule) ) {
                opt.schedule = opt.schedule.getTime();
            }else if ( opt.schedule > 0 ){
                opt.schedule = Date.now() + (parseInt(opt.schedule) * 1000);
            }else{
                opt.schedule = 0;
            }
            this.schedule = parseInt(opt.schedule / 1000);
        },
        getOption: function (){
            return {
                protectKey: this.protectKey,
                priority: this.priority,
                schedule: this.schedule
            }
        },
        /**
         *  enqueueTop(option, callback);
         */
        enqueueTop: function () {
            var option, cb;
            if ( typeof arguments[0] === 'function' ) {
                option = {};
                cb = arguments[0];
            }else{
                option = arguments[0];
                cb = arguments[1];
            }

            this._parseOption(option);
            this._setJob( function (err) {
                if ( err == null ) {
                    model.pushToQueue.bind(this)(this.uuid, true, cb);
                }else{
                    cb(err);
                }
            }.bind(this));
        },
        /**
         *  enqueueAt(new Date | Number, option, callback);
         */
        enqueueAt: function (schedule) {
            var option, cb;
            if ( typeof arguments[1] === 'function' ) {
                option = {};
                cb = arguments[1];
            }else{
                option = arguments[1] || {};
                cb = arguments[2];
            }

            option.schedule = schedule;
            this._parseOption(option);

            this._setJob( function (err) {
                if ( err == null ) {
                    model.pushToSchedule.bind(this)(this.schedule, this.uuid, cb);
                }else{
                    cb(err);
                }
            }.bind(this));
        },
        /**
         *  enqueue(high | med | low | 1 | 0 | -1, option, callback);
         */
        enqueue: function(){
            var option = {}, priority, cb, type;
            for (var i = 0, length = arguments.length; i < length; i+=1) {
                type = typeof arguments[i];
                if ( type === 'object' ) {
                    option = arguments[i];
                }else if ( type === 'function' ) {
                    cb = arguments[i];
                }else{
                    priority = arguments[i];
                }
            };

            option.priority = (priority === 0) ? 0 : priority || option.priority;

            this._parseOption(option);

            this._setJob( function (err) {
                if ( err == null ) {
                    if ( this.schedule > 0 ) {
                        model.pushToSchedule.bind(this)(this.schedule, this.uuid, cb);
                    }else{
                        model.pushToBufferByProtect.bind(this)(this.protectKey, this.uuid, this.priority, cb);
>>>>>>> develop_0.5
                    }
                }else{
                    cb && cb(err);
                }
            }.bind(this));
<<<<<<< HEAD
        },
        requeue: function(){
            var args = arguments;
            this.dequeue(function(err){
                if ( err === null ) {
                    this.enqueue.apply(this, args);
                }else{
                    for (var i = args.length - 1; i >= 0; i-=1) {
                        if ( typeof args[i] === 'function' ) {
                            args[i](err, this);
                            break;
                        }
                    };
                }
            }.bind(this));
        },
        _delJobByKey: function (cb) {
            var key = [ ':job:' + this.uuid, ':job:' + this.uuid + ':timeout'];
            async.each( key, function (item, cb) {
                this._connection.del( this._getPrefix() + item, cb);
            }.bind(this), function (err) {
                cb && cb(err);
                delete key;
            });
        },
        _delJobByQueue: function (cb) {
            var queue = [ ':queue', ':completed', ':failed', ':buffer:' + this.protectKey + ':high', ':buffer:' + this.protectKey + ':med', ':buffer:' + this.protectKey + ':low' ],
                count = 0;
            async.map( queue, function (item, cb) {
                this._connection.lrem(this._getPrefixforProtocol() + item, 0, this.uuid, cb);
            }.bind(this), function (err, result) {
                for (var i = result.length - 1; i > -1; i-= 1) {
                    count += result[i];
                };
                cb && cb( err, count);
                delete queue;
            });
        },
        _checkJobInProcessing: function (cb) {
            this._connection.lrange(this._getPrefixforProtocol() + ':processing', -1000, 1000, function (err, reply) {
                cb(err, reply.indexOf(this.uuid) > -1);
            }.bind(this));
        },
        dequeue: function(cb){
            async.parallel([
                function (cb) {
                    this._delJobByQueue(function(err, count){
                        if ( err === null && count < 1 ) {
                            this._checkJobInProcessing( function (err, bool) {
                                if ( err === null && bool === true ) {
                                    cb('job is processing');
                                }else{
                                    cb(err);
                                }
                            });
                        }else{
                            cb(err, count);                            
                        }
                    }.bind(this));
                }.bind(this),
                this._delJobByKey.bind(this)
            ], cb);
        },
        toCompleted: function(callback){
            callback = callback || function(){};
            async.series([
                this._clean.bind(this),
                function (cb) {
                    this._connection.hset( this._getPrefix() + ':job:' + this.uuid, 'data', JSON.stringify(this.data), cb);
                }.bind(this),
                function (cb) {
                    this._connection.lpush( this._getPrefixforProtocol() + ':completed', this.uuid, cb);
                }.bind(this)
            ], function (err) {
                callback(err, this);
            });
        },
        toFailed: function(callback){
            callback = callback || function(){};
            async.series([
                this._clean.bind(this),
                function (cb) {
                    this._connection.hset( this._getPrefix() + ':job:' + this.uuid, 'data', JSON.stringify(this.data), cb);
                }.bind(this),
                function (cb) {
                    this._connection.lpush( this._getPrefixforProtocol() + ':failed', this.uuid, cb);
=======
        },
        requeueTop: function () {
            var option, cb;
            if ( typeof arguments[0] === 'function' ) {
                option = {};
                cb = arguments[0];
            }else{
                option = arguments[0];
                cb = arguments[1];
            }
            
            async.series([
                this.dequeue.bind(this),
                function (cb) {
                    this.enqueueTop(option, cb);
                }.bind(this)
            ], function (err) {
                cb && cb(err);
            });
        },
        requeueAt: function (schedule) {
            var option, cb;
            if ( typeof arguments[1] === 'function' ) {
                option = {};
                cb = arguments[1];
            }else{
                option = arguments[1];
                cb = arguments[2];
            }
            
            async.series([
                this.dequeue.bind(this),
                function (cb) {
                    this.enqueueAt(schedule, option, cb);
                }.bind(this)
            ], function (err) {
                cb && cb(err);
            });
        },
        requeue: function(){
            var args = Array.prototype.slice.call(arguments), cb;
            if ( typeof args[args.length - 1] === 'function' ) {
                cb = args.pop();
            }

            async.series([
                this.dequeue.bind(this),
                function (cb) {
                    args.push(cb);
                    this.enqueue.apply(this, args);
                }.bind(this)
            ], function (err) {
                cb && cb(err);
            });
        },
        dequeue: function(cb){
            async.waterfall([
                function (cb) {
                    model.cleanJob.bind(this)(this.uuid, cb);
                }.bind(this),
                function (count, cb) {
                    if ( count < 1 ) {
                        model.existsUuidInProcessing.bind(this)( this.uuid, function (err, bool) {
                            cb( err == null && bool === true ? 'job is processing' : err );
                        });
                    }else{
                        cb(null);
                    }
                }.bind(this)
            ], function (err) {
                if ( err == null ) {
                    model.delJob.bind(this)(this.uuid, cb);
                }else{
                    cb(err);
                }
            }.bind(this));
        },
        toCompleted: function(callback){
            async.series([
                function (cb) {
                    model.rmUuidOfProcessing.bind(this)(this.uuid, function (err, reply) {
                        if ( err === null && reply < 1 ) {
                            model.cleanJob.bind(this)(this.uuid, cb);
                        }else{
                            cb(err, reply)
                        }
                    }.bind(this));
                }.bind(this),
                function (cb) {
                    model.setJob.bind(this)(this.uuid, {'data': this.data}, cb);
                }.bind(this),
                function (cb) {
                    model.expireJob.bind(this)(this.uuid, cb);
                }.bind(this),
                function (cb) {
                    model.pushToCompleted.bind(this)(this.uuid, cb);
                }.bind(this),
            ], function (err) {
                callback(err, this);
            });
        },
        toFailed: function(callback){
            async.series([
                function (cb)
                 {
                    model.rmUuidOfProcessing.bind(this)(this.uuid, function (err, reply) {
                        if ( err === null && reply < 1 ) {
                            model.cleanJob.bind(this)(this.uuid, cb);
                        }else{
                            cb(err, reply)
                        }
                    }.bind(this));
                }.bind(this),
                function (cb) {
                    model.setJob.bind(this)(this.uuid, {'data': this.data}, cb);
                }.bind(this),
                function (cb) {
                    model.expireJob.bind(this)(this.uuid, cb);
                }.bind(this),
                function (cb) {
                    model.pushToFailed.bind(this)(this.uuid, cb);
>>>>>>> develop_0.5
                }.bind(this)
            ], function (err) {
                callback(err, this);
            });
        }
    }

    return constructor;

})();