var uuid = require('node-uuid'),
    async = require("async"),
    util = require("util"),
    model = require("../lib/model.js");

module.exports = (function () {

    // protocol, data, option
    // uuid, cb, option
    var constructor = function () {
        var cb = arguments[1];
        var option = arguments[2];
        var fireSelf = arguments[3];

        fireSelf._apply(this, option);

        if ( typeof arguments[1] !== 'function' ){
            this.uuid = uuid.v4();
            this.protocol = (arguments[0] && arguments[0].toString()) || 'universal';
            this.data = arguments[1] || '';
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
                    }
                }else{
                    cb && cb(err);
                }
            }.bind(this));
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
                }.bind(this)
            ], function (err) {
                callback(err, this);
            });
        }
    }

    return constructor;

})();