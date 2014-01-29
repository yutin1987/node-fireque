var uuid = require('node-uuid');
var redis = require("redis");

module.exports = (function () {

    var constructor = function (protocol, data, option) {
        this.uuid = uuid.v4();

        this.protocol = (protocol && protocol.toString()) || 'universal';
        this.data = data || '';
        this.timeout = (option && option.timeout) || 30;
        this._connection = (option && option.connection) || redis.createClient(
            (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
            (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
        );

        return this;
    }

    constructor.prototype = {
        uuid: '',
        protocol: 'universal',
        data: '',
        timeout: '',
        _connection: null,
        enqueue: function(callback, priority){
            var self = this;
            var queueName = Fireque._getQueueName();
            var dataKey = queueName + ':job:' + self.uuid;
            var ready = 3;
            var error = [];

            var doEnqueue = function(err, reply){
                if ( err !== null ) {
                    error.push(err);
                }
                ready -= 1;
                if ( ready < 1 ) {
                    if ( error.length ) {
                        callback(error, self);
                    }else{
                        self._connection.lpush( queueName + ':' + self.protocol + ':queue:' + (priority || 'med'), self.uuid, function(err, reply) {
                            callback(err, self);
                        });
                    }
                }
            }

            self._connection.hset( dataKey, 'data', JSON.stringify(self.data), doEnqueue);
            self._connection.hset( dataKey, 'timeout', self.timeout, doEnqueue);
            self._connection.expire( dataKey, 3 * 24 * 60 * 60, doEnqueue);
        },
        requeue: function(callback, priority){
            var self = this;
            var queueName = Fireque._getQueueName();

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
        }
    }

    return constructor;

})();