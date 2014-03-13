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
            priority = (this._priority[protectKey] && this._priority[protectKey].length > 0) ? this._priority[protectKey] : this.priority.concat();

            return priority;
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
        _fetchLowWorklandForProtect: function (cb) {
            var protectKey = [];
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
                    }.bind(this));
                }
                cb(err, protectKey);
            }.bind(this));
        },
        _popBufferToQueueByProtect: function (protectKey, priority, cb) {
            var task, index;
            async.series([
                function (cb) {
                    this._getLicenseByProtect.bind(this)(protectKey, cb);
                }.bind(this),
                function (cb) {
                    model.popFromBufferByProtect.bind(this)(protectKey, priority, function (err, uuid, from) {
                        if ( err === null && uuid) {
                            task = {'uuid': uuid, 'from': from};
                            cb(null);
                        }else{
                            cb(err || true);
                        }
                    }.bind(this));
                }.bind(this),
                function (cb) {
                    model.pushToQueue.bind(this)(task.uuid, cb);
                }.bind(this)
            ], function (err, result) {
                if (err !== null) {
                    switch (result.length) {
                        case 3: model.pushToBufferByProtect.bind(this)(protectKey, task.from);
                        case 2: this._returnLiccenseByProtect(protectKey);
                    }
                }
                cb(err, task);

                delete task;
            }.bind(this));
        },
        _listenBuffer: function (cb) {
            this._fetchLowWorklandForProtect( function (err, protectKey) {
                async.map(protectKey, function (item, cb) {
                    var index, doPop, total = {high: 0, med: 0, low: 0};
                    (doPop = function () {
                        this._priority[item] = this._fetchPriorityByProtect(item);
                        this._popBufferToQueueByProtect(item, this._priority[item], function (err, task) {
                            if ( err == null && task ) {
                                index = this._priority[item].indexOf(task.from);
                                if ( index > -1 ) {
                                    this._priority[item].splice(index, 1);
                                }else{
                                    this._priority[item] = this.priority.concat()
                                }
                                if ( total[task.from] !== undefined ) {
                                    total[task.from] += 1;
                                }
                                doPop();
                            }else{
                                cb(null, total);
                            }
                        }.bind(this));
                    }.bind(this))();
                }.bind(this), function (err, result) {
                    var total = {high: 0, med: 0, low: 0};
                    for (var i = 0, length = result.length; i < length; i+=1) {
                        if ( result[i] ) {
                            total['high'] += result[i].high;
                            total['med'] += result[i].med;
                            total['low'] += result[i].low;
                        }
                    };

                    process.nextTick( function () {
                        cb && cb(null, total);
                    });

                    delete result;
                    delete protectKey;
                });
            }.bind(this));
        },
        _popScheduleToBuffer: function(timestamp, cb){
            async.waterfall([
                function (cb) {
                    model.popUuidFromSchedule.bind(this)(timestamp, function (err, uuid) {
                        cb(err || uuid == null || null, uuid);
                    });
                }.bind(this),
                function (uuid, cb) {
                    model.getJob.bind(this)(uuid, function (err, job) {
                        cb(err || job == null || null, uuid, job);
                    });
                }.bind(this),
                function (uuid, job, cb) {
                    model.pushToBufferByProtect.bind(this)(job.protectKey, uuid, job.priority, function (err) {
                        cb(err, uuid);
                    });
                }.bind(this)
            ], function (err, uuid) {
                cb(err || uuid == null || null, uuid);
            });
        },
        _listenSchedule: function(cb){
            var total = {};
            model.fetchScheduleByTimestamp.bind(this)(Date.now() / 1000, function (err, timestamp) {
                if ( err == null && timestamp && timestamp.length ) {
                    async.map(timestamp, function (item, cb) {
                        var count = 0, doPop;
                        (doPop = function () {
                            this._popScheduleToBuffer(item, function (err) {
                                if ( err == null && count < 300 ) {
                                    count += 1;
                                    doPop();
                                }else{
                                    cb(null, count);
                                }
                            }.bind(this));
                        }.bind(this))();
                    }.bind(this), function (err, result) {
                        timestamp.forEach(function (time, i) {
                            total[time] = result[i];
                        });
                        cb(err, total);
                    });
                }else{
                    cb(err, total);
                }
            }.bind(this));
        },
        start: function(cb, interval){
            if ( cb && typeof cb === 'number' ) {
                interval = cb;
            }
            if ( interval === undefined ) {
                interval = 2;
            }

            this._serviceId = setInterval(function(){
                if ( this._doListenBuffer === false ) {
                    this._doListenBuffer = true;
                    async.parallel({
                        buffer: this._listenBuffer.bind(this),
                        schedule: this._listenSchedule.bind(this)
                    }, function (err, result) {
                        process.nextTick( function () {
                            cb && cb(err, result);
                        });
                        this._doListenBuffer = false;
                    }.bind(this));
                }
            }.bind(this), interval * 1000);

            return this;
        },
        stop: function(){
            clearInterval(this._serviceId);
        }
    }

    return constructor;

})();