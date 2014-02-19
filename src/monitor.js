var redis = require("redis"),
    async = require("async");

module.exports = (function () {

    var constructor = function (protocol, workload, option) {
        this.protocol = (protocol && protocol.toString()) || 'universal';

        this.workload = (workload && parseInt(workload, 10)) || 5;

        this.priority = (option && option.priority) || this.priority.concat();

        this._priority = {};

        this._connection = (option && option.connection) || redis.createClient(
            (option && option.port) || Fireque.FIREQUE_PORT ||  6379,
            (option && option.host) || Fireque.FIREQUE_HOST || '127.0.0.1'
        );

        return this;
    }

    constructor.prototype = {
        protocol: 'universal',
        workload: 5,
        priority: ['high', 'high', 'high', 'med', 'med', 'low'],
        _serviceId: 0,
        _doListenBuffer: false,
        _priority: {},
        _connection: null,
        _getPrefix: function(){
            return Fireque._getQueueName() + ':' + this.protocol;
        },
        _fetchCollapseFromBuffer: function (cb) {
            this._connection.keys(this._getPrefix()+':buffer:*', function(err, reply){
                var item, collapse = [];
                if ( err === null ){
                    for (var i = 0, length = reply.length; i < length; i+=1) {
                        item = reply[i].split(':')[4];
                        if ( item && collapse.indexOf(item) < 0 ) {
                            collapse.push(item);
                        }
                    }
                }

                process.nextTick(function () {
                    cb(err || collapse.length < 1 || null, collapse);
                });

                delete reply;
                delete collapse;
            });
        },
        _fetchPriorityByCollapse: function(collapse, cb) {
            var item, priority = {};
            if ( collapse && collapse.length ) {
                for (var i = 0, length=collapse.length; i < length; i+=1) {
                    item = collapse[i];
                    priority[item] = (this._priority[item] && this._priority[item].length > 0) ? _priority[item] : this.priority;
                };
            }

            process.nextTick(function () {
                cb(null, priority);
            });

            delete collapse;
            delete priority;
        },
        _filterLowWorkloadbyCollapse: function (collapse, cb) {
            keys = [];
            
            for (var i = 0, length = collapse.length; i < length; i++) {
                keys.push(this._getPrefix() + ':workload:' + collapse[i]);
            };

            this._connection.mget(keys, function (err, reply){
                var workload = [];
                if ( err === null ) {
                    for (var i = 0, length = reply.length; i < length; i+=1) {
                        if ( collapse[i] && reply[i] < this.workload) {
                            workload.push(collapse[i]);
                        }
                    };
                }

                process.nextTick(function () {
                    cb(err, workload);
                });

                delete reply;
                delete collapse;
                delete workload;
            }.bind(this));

            delete keys;
        },
        _getLicenseByCollapse: function (collapse, cb) {
            this._connection.incr(this._getPrefix() + ':workload:' + collapse, function (err, reply) {
                if ( err === null && reply > this.workload) {
                    this._returnLiccenseByCollapse(collapse);
                }
                cb(err || reply > this.workload || null);
            }.bind(this));
        },
        _returnLiccenseByCollapse: function (collapse, cb) {
            this._connection.incrby(this._getPrefix() + ':workload:' + collapse, -1, function (err) {
                if ( cb && typeof cb === 'function' ) {
                    cb(err);
                }
            });
        },
        _getUuidFromBufferByCollapse: function (collapse, cb) {
            var priority = (this._priority[collapse] || []).concat(['high', 'med', 'low']);
            for (var i = 0, length=priority.length; i < length; i+=1) {
                priority[i] = this._getPrefix() + ':buffer:' + collapse + ':' + priority[i];
            };
            priority.push(1); // push waiting sec.

            this._connection.brpop(priority, function (err, reply) {
                var task = {};
                if ( err === null && reply[0] ) {
                    task.priority = reply[0].split(':')[5];
                    task.uuid = reply[1];
                }
                cb(err || !reply[0] || null, task);

                delete task;
                delete reply;
            });

            delete priority;
        },
        _pushUuidToQueue: function (uuid, cb) {
            this._connection.lpush(this._getPrefix() + ':queue', uuid, cb);
        },
        _popBufferToQueueByCollapse: function (collapse, cb) {
            var task;
            async.series([
                function (cb) {
                    this._getLicenseByCollapse(collapse, cb);
                }.bind(this),
                function (cb) {
                    this._getUuidFromBufferByCollapse(collapse, function(err) {
                        var index;
                        if ( err === null ) {
                            task = arguments[1];
                            index = this._priority[collapse].indexOf(task.priority);
                            if ( index > -1 ) {
                                this._priority[collapse].splice(index, 1);
                            }
                        }

                        cb(err);
                    }.bind(this));
                }.bind(this),
                function (cb) {
                    this._pushUuidToQueue(task.uuid, cb);
                }.bind(this)
            ], function (err, result) {
                if (err !== null) {
                    switch (result.length) {
                        case 3:
                            this._connection.lpush(this._getPrefix() + ':buffer:' + collapse + ':' + from);
                        case 2:
                            this._returnLiccenseByCollapse(collapse);
                    }
                }
                cb(err, task);

                delete task;
            });
        },
        _listenBuffer: function (cb) {
            async.waterfall([
                this._fetchCollapseFromBuffer.bind(this),
                function (collapse, cb) {
                    async.parallel({
                        priority: function (cb) {
                            this._fetchPriorityByCollapse(collapse, cb);
                        }.bind(this),
                        workload: function (cb) {
                            this._filterLowWorkloadbyCollapse(collapse, cb);
                        }.bind(this)
                    }, function (err, result) {
                        if ( err === null ) {
                            this._priority = result.priority;
                        }
                        cb(err, result.workload);
                        delete collapse;
                        delete results;
                    }.bind(this));
                }.bind(this),
                function (workload, cb) {
                    async.map(workload, function (collapse, cb) {
                        this._popBufferToQueueByCollapse(collapse, function(err, task) {
                            cb( null, task);
                        });
                    }.bind(this), function (err, result) {
                        cb(err, result);
                        delete workload;
                    });
                }.bind(this)
            ], function (err, result) {
                cb(err, result);
                delete result;
            });
        },
        start: function(sec){
            if ( sec === undefined ) {
                sec = 2;
            }

            this._serviceId = setinterval(function(){
                if ( this._doListenBuffer === false ) {
                    this._doListenBuffer = true;
                    this._listenBuffer(function(err, result){
                        if ( err === null ) {
                            var priority = { high: 0, med: 0, low: 0};
                            for (var i = 0, length = result.length; i < length; i++) {
                                priority[result[i].priority] += 1;
                            };
                            console.log('PUSH> ', 'high:', priority.high, 'med:', priority.med, 'low:', priority.low);
                        }else{
                            console.log(err, result);
                        }
                        this._doListenBuffer = false;
                    }.bind(this));
                }
            }.bind(this), sec * 1000);
        },
        stop: function(){
            clearInterval(this._serviceId);
        }
    }

    return constructor;

})();