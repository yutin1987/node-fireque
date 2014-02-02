var uuid = require('node-uuid');
var redis = require("redis");

module.exports = (function () {

    var constructor = function (protocol, option) {
        if ( protocol ) {
            this.protocol = (typeof protocol === 'object' && protocol.length) ? protocol : [protocol.toString()];
        }
        this._wait = (option && option.wait) || this._wait;
        this._connection = (option && option.connection) || redis.createClient(
            (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
            (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
        );

        return this;
    }

    constructor.prototype = {
        protocol: ['universal'],
        _wait: 10,
        _connection: null,
        onCompleted: function(action, max_count){
            var doFetch,
                wait = this._wait,
                keys = [];
            for (var i = 0, length = this.protocol.length; i < length; i += 1) {
                keys.push(Fireque._getQueueName() + ':' + this.protocol[i] + ':completed');
            };
            (doFetch = function(self, keys, endTimes){
                var completed_list = [];
                var doListen, doReady;
                doReady = function(){
                    if( new Date().getTime() > endTimes || completed_list.length >= max_count ){
                        if ( completed_list.length > 0 ) {
                            action(completed_list);
                        }
                        doFetch(self, keys, new Date().getTime() + (wait * 1000) );
                    }else{
                        doListen();
                    }
                };
                (doListen = function(){
                    var times = Math.ceil( (endTimes - new Date().getTime()) / 1000 );
                    if ( times < 1 ){
                        times = 1;
                    }
                    self._connection.brpop( keys, times, function(err, reply) {
                        if ( err === null && reply ) {
                            completed_list.push(new Fireque.Job(reply[1], doReady, {
                                connection: self._connection
                            }));
                        }else{
                            doReady();
                        }
                    });
                })();
            })(this, keys, new Date().getTime() + (wait * 1000) );
        },
        onFailed: function (action, max_count) {
            var doFetch,
                wait = this._wait,
                keys = [];
            for (var i = 0, length = this.protocol.length; i < length; i += 1) {
                keys.push(Fireque._getQueueName() + ':' + this.protocol[i] + ':failed');
            };
            (doFetch = function(self, keys, endTimes){
                var completed_list = [];
                var doListen, doReady;
                doReady = function(){
                    if( new Date().getTime() > endTimes || completed_list.length >= max_count ){
                        if ( completed_list.length > 0 ) {
                            action(completed_list);
                        }
                        doFetch(self, keys, new Date().getTime() + (wait * 1000) );
                    }else{
                        doListen();
                    }
                };
                (doListen = function(){
                    var times = Math.ceil( (endTimes - new Date().getTime()) / 1000 );
                    if ( times < 1 ){
                        times = 1;
                    }
                    self._connection.brpop( keys, times, function(err, reply) {
                        if ( err === null && reply ) {
                            completed_list.push(new Fireque.Job(reply[1], doReady, {
                                connection: self._connection
                            }));
                        }else{
                            doReady();
                        }
                    });
                })();
            })(this, keys, new Date().getTime() + (wait * 1000) );
        },
        onTimeout: function () {

        }

    }

    return constructor;

})();