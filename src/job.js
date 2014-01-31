var uuid = require('node-uuid');
var redis = require("redis");

module.exports = (function () {

    var constructor = function (protocol, data, option) {
        var self = this;
        if ( typeof data !== 'function' ){
            self.uuid = uuid.v4();

            self.protocol = (protocol && protocol.toString()) || 'universal';
            self.data = data || '';
            self.timeout = (option && option.timeout) || self.timeout;
            self._connection = (option && option.connection) || redis.createClient(
                (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
                (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
            );
        }else{
            var callback = data;
            var queueName = Fireque._getQueueName();

            self.uuid = protocol;

            self._connection = (option && option.connection) || redis.createClient(
                (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
                (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
            );

            self._connection.hgetall(queueName + ':job:' + self.uuid, function(err, reply){
                var key, value;
                if ( err === null && reply){
                    for(key in reply){ 
                        value = reply[key];
                        if ( key == 'data' ){
                            self['data'] = JSON.parse(value);
                        }else{
                            self[key] = value;
                        }
                    }
                }
                callback(err, self);
            });
        }

        return self;
    }

    constructor.prototype = {
        uuid: '',
        protocol: 'universal',
        data: '',
        timeout: 30,
        _connection: null,
        _clean: function(callback) {
            var self = this;
            self._connection.lrem(Fireque._getQueueName() + ':' + self.protocol + ':processing', 0, self.uuid, function(err, reply){
                if ( reply < 1 ) {
                    self.dequeue(function(err){
                        callback(err, self);
                    });
                }else{
                    callback(null, self);
                }
            });
        },
        enqueue: function(callback, priority){
            var self = this;
            var queueName = Fireque._getQueueName();
            var dataKey = queueName + ':job:' + self.uuid;

            self._connection.hmset( dataKey,
                'data', JSON.stringify(self.data),
                'timeout', self.timeout,
                'protocol', self.protocol,
            function(err, reply){
                if ( err !== null ) {
                    callback(err, self);
                }else{
                    self._connection.expire( dataKey, 3 * 24 * 60 * 60);
                    self._connection.lpush( queueName + ':' + self.protocol + ':queue:' + (priority || 'med'), self.uuid, function(err, reply) {
                        callback(err, self);
                    });
                }
            });
        },
        requeue: function(callback, priority){
            var self = this;

            self.dequeue(function(err){
                if ( err === null ) {
                    self.enqueue(callback, priority);
                }else{
                    callback(err, self);
                }
            });
        },
        dequeue: function(callback){
            var self = this;
            var queueName = Fireque._getQueueName();
            var ready = 5;
            var count = 0;
            var error = [];

            var doDequeue = function(err, reply){
                if ( err !== null ) {
                    error.push(err);
                }
                count += reply;
                ready -= 1;
                if ( ready < 1 ) {
                    if ( error.length ) {
                        callback(error, self);
                    }else if ( count < 1 ){
                        callback('this job not exists, maybe is processing...', self);
                    }else{
                        callback(null, self);
                    }
                }
            }

            self._connection.lrem(queueName + ':' + self.protocol + ':queue:high', 0, self.uuid, doDequeue);
            self._connection.lrem(queueName + ':' + self.protocol + ':queue:med', 0, self.uuid, doDequeue);
            self._connection.lrem(queueName + ':' + self.protocol + ':queue:low', 0, self.uuid, doDequeue);
            self._connection.lrem(queueName + ':' + self.protocol + ':completed', 0, self.uuid, doDequeue);
            self._connection.lrem(queueName + ':' + self.protocol + ':failed', 0, self.uuid, doDequeue);
        },
        completed: function(callback){
            var self = this;
            var callback = callback || function(){};
            self._clean(function(err){
                if ( err === null ) {
                    self._connection.lpush( Fireque._getQueueName() + ':' + self.protocol + ':completed', self.uuid, function(err, reply) {
                        callback(err, self);
                    });
                }else{
                    callback(err, self);
                }
            });
        },
        failed: function(callback){
            var self = this;
            var callback = callback || function(){};
            self._clean(function(err){
                if ( err === null ) {
                    self._connection.lpush( Fireque._getQueueName() + ':' + self.protocol + ':failed', self.uuid, function(err, reply) {
                        callback(err, self);
                    });
                }else{
                    callback(err, self);
                }
            });
        }
    }

    return constructor;

})();