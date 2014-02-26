var async = require("async"),
    model = require("../lib/model.js");

module.exports = (function () {

    var constructor = function (protocol, workload, option, fireSelf) {

        fireSelf._apply(this, option);

        this._fireSelf = fireSelf;

        this.protocol = (protocol && protocol.toString()) || 'universal';

        this.workload = (workload && parseInt(workload, 10)) || 5;

        this.priority = (option && option.priority) || this.priority.concat();

        this._priority = {};

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
        _fetchPriorityByProtect: function(protectKey, cb) {
            var item, priority = {};
            if ( protectKey && protectKey.length ) {
                protectKey.forEach( function (item) {
                    priority[item] = (this._priority[item] && this._priority[item].length > 0) ? this._priority[item] : this.priority.concat();
                }.bind(this));
            }

            process.nextTick(function () {
                cb(null, priority);
            });

            delete protectKey;
            delete priority;
        },
        _filterLowWorkloadbyProtect: function (protectKey, cb) {
            keys = [];
            
            for (var i = 0, length = protectKey.length; i < length; i++) {
                keys.push(this._getPrefix() + ':workload:' + protectKey[i]);
            };

            this._connection.mget(keys, function (err, reply){
                var workload = [];
                if ( err === null ) {
                    for (var i = 0, length = reply.length; i < length; i+=1) {
                        if ( protectKey[i] && reply[i] < this.workload) {
                            workload.push(protectKey[i]);
                        }
                    };
                }

                process.nextTick(function () {
                    cb(err, workload);
                });

                delete reply;
                delete protectKey;
                delete workload;
            }.bind(this));

            delete keys;
        },
        _getLicenseByProtect: function (protectKey, cb) {
            model.incrementWorkload.bind(this)(protectKey, function (err, workload) {
                if ( err === null && workload > this.workload) {
                    this._returnLiccenseByProtect(protectKey);
                }
                cb(err || workload > this.workload || null);
            }.bind(this));
        },
        _returnLiccenseByProtect: function (protectKey, cb) {
            model.decrementWorkload.bind(this)(protectKey, cb);
        },
        _pushUuidToQueue: function (uuid, cb) {
            this._connection.lpush(this._getPrefix() + ':queue', uuid, cb);
        },
        _popBufferToQueueByProtect: function (protectKey, cb) {
            var task, index;
            async.series([
                function (cb) {
                    this._getLicenseByProtect.bind(this)(protectKey, cb);
                }.bind(this),
                function (cb) {
                    model.popFromBufferByProtect.bind(this)(protectKey, this._priority[protectKey], function (err, uuid, from) {
                        if ( err === null && uuid) {
                            task = {'uuid': uuid, 'from': from};
                            index = this._priority[protectKey].indexOf(task.from);
                            if ( index > -1 ) {
                                this._priority[protectKey].splice(index, 1);
                            }
                        }
                        cb(err);
                    }.bind(this));
                }.bind(this),
                function (cb) {
                    model.pushToQueue.bind(this)(task.uuid, cb);
                }.bind(this)
            ], function (err, result) {
                if (err !== null) {
                    switch (result.length) {
                        case 3:
                            model.pushToBufferByProtect.bind(this)(protectKey, task.bind);
                        case 2:
                            this._returnLiccenseByProtect(protectKey);
                    }
                }
                cb(err, task);

                delete task;
            }.bind(this));
        },
        _listenBuffer: function (cb) {
            async.parallel({
                protect: function (cb) {
                    model.fetchProtectFromBuffer.bind(this)(cb);
                }.bind(this),
                workload: function (cb) {
                    model.fetchOverWorkload.bind(this)(this.workload, cb);
                }.bind(this)
            }, function (err, result) {
                if ( err == null ) {
                    protectKey = result.protect.filter(function (item) {
                        return (result.workload[item] && result.workload[item] >= this.workload) ? false : true;
                    });
                }

                process.nextTick( function () {
                    this._fetchPriorityByProtect(protectKey, function (err, priority) {
                        if ( err === null ) {
                            this._priority = priority;
                            async.map(protectKey, function (item, cb) {
                                this._popBufferToQueueByProtect(item, cb);
                            }.bind(this), function (err, result) {
                                cb(err, result);
                                delete result;
                                delete protectKey;
                            });
                        }else{
                            cb(err);
                        }
                    }.bind(this));
                }.bind(this));

                delete result;
            }.bind(this));
        },
        start: function(sec){
            if ( sec === undefined ) {
                sec = 2;
            }

            this._serviceId = setInterval(function(){
                if ( this._doListenBuffer === false ) {
                    this._doListenBuffer = true;
                    this._listenBuffer(function(err, result){
                        if ( err == null ) {
                            var priority = { high: 0, med: 0, low: 0};
                            result && result.forEach(function (task) {
                                priority[task.from] += 1;
                            });
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