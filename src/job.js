var uuid = require('node-uuid'),
    async = require("async"),
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
            this.protectKey = (option && option.protectKey) || this.protectKey;
            this.priority = (option && option.priority) || this.priority;
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
        enqueue: function(){
            var protectKey, priority, cb, focus, type;

            for (var i = arguments.length - 1; i >= 0; i-=1) {
                type = typeof arguments[i];
                if ( type === 'function' ) {
                    cb = arguments[i];
                }else if ( type === 'boolean') {
                    focus = arguments[i];
                }else if ( arguments[i] === 'high' || arguments[i] === 'med' || arguments[i] === 'low' ) {
                    priority = arguments[i];
                }else{
                    protectKey = arguments[i];
                }
            };

            this.priority = priority || this.priority;
            this.protectKey = protectKey || this.protectKey;

            async.series([
                function (cb) {
                    model.setJob.bind(this)(this.uuid, {
                        'data': this.data,
                        'protectKey': this.protectKey,
                        'protocol': this.protocol,
                        'priority': this.priority
                    }, cb);
                }.bind(this),
                function (cb) {
                    model.expireJob.bind(this)(this.uuid, cb);
                }.bind(this),
                function (cb) {
                    if ( focus === true ) {
                        model.pushToQueue.bind(this)(this.uuid, true, cb);
                    }else{
                        model.pushToBufferByProtect.bind(this)(this.protectKey, this.uuid, this.priority, cb);
                    }
                }.bind(this)
            ], function (err) {
                cb && cb(err, this);
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
        dequeue: function(cb){
            async.parallel([
                function (cb) {
                    model.cleanJob.bind(this)( this.uuid, function (err, count) {
                        if ( err === null && count < 1 ) {
                            model.existsUuidInProcessing.bind(this)( this.uuid, function (err, bool) {
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
                function (cb) {
                    model.delJob.bind(this)(this.uuid, cb);
                }.bind(this)
            ], cb);
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
                }.bind(this)
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