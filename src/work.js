var uuid = require('node-uuid');
var redis = require("redis");

module.exports = (function () {

    var constructor = function (protocol, option) {
        this.protocol = (protocol && protocol.toString()) || 'universal';
        this.workload = (option && option.workload) || this.workload;
        this._wait = (option && option.wait) || this._wait;
        this._priority = (option && option.priority) || this._priority;
        this._connection = (option && option.connection) || redis.createClient(
            (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
            (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
        );

        return this;
    }

    constructor.prototype = {
        protocol: 'universal',
        workload: 100,
        _wait: 10,
        _priority: ["high", "high", "high", "med", "med", "low"],
        _connection: null,
        _wrokers: [],
        onPerform: function(worker) {
            var self = this,
                workload = self.workload,
                queueName = Fireque._getQueueName(),
                listenKeys = [],
                defaultKeys = [];

            for (var i = 0, length = self._priority.length; i < length; i += 1) {
                listenKeys.push(queueName + ':' + self.protocol + ':queue:' + self._priority[i]);
            };
            defaultKeys.push(queueName + ':' + self.protocol + ':queue:high');
            defaultKeys.push(queueName + ':' + self.protocol + ':queue:med');
            defaultKeys.push(queueName + ':' + self.protocol + ':queue:low');

            self._wrokers.push(worker);

            var doNext, doListen, doPerform;

            (doNext = function(keys) {
                if ( workload > 0 && self._wrokers.indexOf(worker) > -1) {
                    if ( keys.length > 0 ) {
                        doListen(keys);
                    }else{
                        doListen(listenKeys);
                    }
                }
            });

            (doPerform = function(keys, uuid) {
                workload -= 1;
                new Fireque.Job(uuid, function(err, job){
                    try{
                        if ( err !== null ) {
                            throw (new Error('get Job data error.'))
                        }

                        worker(job, function(status){
                            if ( status === true ) {
                                job.completed();
                            }else if ( status === false ) {
                                job.failed();
                            }
                            doNext(keys);
                        });
                    }catch(e){
                        // e.message || e
                        job.failed();
                        doNext(keys);
                    }
                }, {
                    connection: self._connection
                });
            });

            (doListen = function(keys) {
                var key, uuid;
                self._connection.brpop( [].concat(keys, defaultKeys, self._wait), function(err, reply){
                    if ( err === null && reply ) {
                        key = reply[0];
                        uuid = reply[1];
                        self._connection.lpush( queueName + ':' + self.protocol + ':processing', uuid);
                        index = keys.indexOf(key);
                        if ( index > -1 ) {
                            keys.splice(index, 1);
                        }
                        doPerform(keys, uuid);
                    }else{
                        doNext(keys);
                    }
                });
            })(listenKeys);
        },
        offPerform: function(worker){
            if ( worker === undefined ) {
                while(this._wrokers.length > 0){
                    this._wrokers.pop();
                }
            }else{
                var index = this._wrokers.indexOf(worker);
                if ( index > -1 ) {
                    this._wrokers.splice(index, 1);
                }
            }
        },
        exit: function () {

        }
    }

    return constructor;

})();