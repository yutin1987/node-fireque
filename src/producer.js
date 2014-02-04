var uuid = require('node-uuid');
var redis = require("redis");

module.exports = (function () {

    var jobs = {};

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
        _event_completed: [],
        _event_failed: [],
        _event_timeout: [],
        onCompleted: function(process, max_count){
            var self = this,
                wait = this._wait,
                keys = [],
                doFetch, doListen, doAssign;

            max_count = max_count || 10;

            for (var i = 0, length = this.protocol.length; i < length; i += 1) {
                keys.push(Fireque._getQueueName() + ':' + this.protocol[i] + ':completed');
            };

            this._event_completed.push(process);

            (doAssign = function(endTimes, list, uuid) {
                list.push(new Fireque.Job(uuid, function(){
                    doFetch(endTimes, list);
                }, {
                    connection: self._connection
                }));
            });

            (doListen = function(endTimes, list) {
                var times = Math.ceil( (endTimes - new Date().getTime()) / 1000 );
                if ( times < 1 ){
                    times = 1;
                }
                self._connection.brpop( keys, times, function(err, reply) {
                    if ( err === null && reply ) {
                        doAssign(endTimes, list, reply[1]);
                    }else{
                        doFetch(endTimes, list);
                    }
                });

            });

            (doFetch = function(endTimes, list) {
                if ( new Date().getTime() > endTimes || list.length >= max_count ) {
                    if ( list.length > 0 ) {
                        process(list);
                    }
                    if ( self._event_completed.indexOf(process) > -1 ){
                        doListen( new Date().getTime() + (wait * 1000), []);
                    }
                }else{
                    doListen(endTimes, list);
                }
            })(0, []);
        },
        offCompleted: function(process){
            if ( process === undefined ) {
                while(this._event_completed.length > 0){
                    this._event_completed.pop();
                }
            }else{
                var index = this._event_completed.indexOf(process);
                if ( index > -1 ) {
                    this._event_completed.splice(index, 1);
                }
            }
        },
        onFailed: function(process, max_count){
            var self = this,
                wait = this._wait,
                keys = [],
                doFetch, doListen, doAssign;

            max_count = max_count || 10;
            
            for (var i = 0, length = this.protocol.length; i < length; i += 1) {
                keys.push(Fireque._getQueueName() + ':' + this.protocol[i] + ':failed');
            };

            this._event_failed.push(process);

            (doAssign = function(endTimes, list, uuid) {
                list.push(new Fireque.Job(uuid, function(){
                    doFetch(endTimes, list);
                }, {
                    connection: self._connection
                }));
            });

            (doListen = function(endTimes, list) {
                var times = Math.ceil( (endTimes - new Date().getTime()) / 1000 );
                if ( times < 1 ){
                    times = 1;
                }
                self._connection.brpop( keys, times, function(err, reply) {
                    if ( err === null && reply ) {
                        doAssign(endTimes, list, reply[1]);
                    }else{
                        doFetch(endTimes, list);
                    }
                });

            });

            (doFetch = function(endTimes, list) {
                if ( new Date().getTime() > endTimes || list.length >= max_count ) {
                    if ( list.length > 0 ) {
                        process(list);
                    }
                    if ( self._event_failed.indexOf(process) > -1 ){
                        doListen( new Date().getTime() + (wait * 1000), []);
                    }
                }else{
                    doListen(endTimes, list);
                }
            })(0, []);
        },
        offFailed: function(process){
            if ( process === undefined ) {
                while(this._event_failed.length > 0){
                    this._event_failed.pop();
                }
            }else{
                var index = this._event_failed.indexOf(process);
                if ( index > -1 ) {
                    this._event_failed.splice(index, 1);
                }
            }
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