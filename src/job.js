var uuid = require('node-uuid');
    redis = require("redis");
    async = require("async");

module.exports = (function () {

    // protocol, data, option
    // uuid, cb, option
    var constructor = function () {
        var callback = arguments[1] || function(){};

        var option = arguments[2];
        this._connection = (option && option.connection) || redis.createClient(
            (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
            (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
        );

        if ( typeof arguments[1] !== 'function' ){
            this.uuid = uuid.v4();
            this.protocol = (arguments[0] && arguments[0].toString()) || 'universal';
            this.data = arguments[1] || '';
            this.collapse = (option && option.collapse) || this.collapse;
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
            }.bind(this));
        }

        return this;
    }

    constructor.prototype = {
        uuid: '',
        protocol: 'universal',
        data: '',
        collapse: 'unrestricted',
        priority: 'med',
        _connection: null,
        _getPrefix: function(){
            return Fireque._getQueueName() + ':' + this.protocol;
        },
        _expire: function(){
            this._connection.expire( this._getPrefix() + ':job:' + this.uuid, 3 * 24 * 60 * 60);
        },
        _clean: function(callback) {
            this._connection.lrem(this._getPrefix() + ':processing', 0, this.uuid, function(err, reply){
                if ( err === null && reply < 1 ) {
                    this.dequeue(callback);
                }else{
                    callback(err, this);
                }
            }.bind(this));
        },
        enqueue: function(){
            var collapse, priority, callback, queue, type;

            for (var i = arguments.length - 1; i >= 0; i-=1) {
                type = typeof arguments[i];
                if ( type === 'function' ) {
                    callback = arguments[i];
                }else if ( type === 'boolean') {
                    collapse = arguments[i];
                }else if ( arguments[i] === 'high' || arguments[i] === 'med' || arguments[i] === 'low' ) {
                    priority = arguments[i];
                }else{
                    collapse = arguments[i];
                }
            };

            callback = callback || function(){};
            this.priority = priority || this.priority;
            this.collapse = collapse || (collapse === false ? collapse : this.collapse);

            this._connection.hmset( this._getPrefix() + ':job:' + this.uuid,
                'data', JSON.stringify(this.data),
                'collapse', this.collapse,
                'protocol', this.protocol,
                'priority', this.priority,
            function(err, reply){
                if ( err !== null ) {
                    callback(err, this);
                }else{
                    this._expire();
                    if ( collapse === false ) {
                        queue = this._getPrefix() + ':queue';
                    }else{
                        queue = this._getPrefix() + ':buffer:' + this.collapse + ':' + this.priority;
                    }
                    this._connection.lpush( queue, this.uuid, function(err, reply) {
                        callback(err, this);
                    }.bind(this));
                }
            }.bind(this));
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
            var key = [ ':job:' + this.uuid, ':timeout:' + this.uuid];
            async.each( key, function (item, cb) {
                this._connection.del( this._getPrefix() + item, cb);
            }.bind(this), function (err) {
                cb && cb(err);
                delete key;
            });
        },
        _delJobByQueue: function (cb) {
            var queue = [ ':queue', ':completed', ':failed', ':buffer:' + this.collapse + ':high', ':buffer:' + this.collapse + ':med', ':buffer:' + this.collapse + ':low' ],
                count = 0;
            async.map( queue, function (item, cb) {
                this._connection.lrem(this._getPrefix() + item, 0, this.uuid, cb);
            }.bind(this), function (err, result) {
                for (var i = result.length - 1; i > -1; i-= 1) {
                    count += result[i];
                };
                cb && cb( err, count);
                delete queue;
            });
        },
        _checkJobInProcessing: function (cb) {
            this._connection.lrange(this._getPrefix() + ':processing', -1000, 1000, function (err, reply) {
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
                    this._connection.lpush( this._getPrefix() + ':completed', this.uuid, cb);
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
                    this._connection.lpush( this._getPrefix() + ':failed', this.uuid, cb);
                }.bind(this)
            ], function (err) {
                callback(err, this);
            });
        }
    }

    return constructor;

})();