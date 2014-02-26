var util = require('util'),
    async = require("async");

/**
 * Expose the root command.
 */

exports = module.exports = {};


// Queue

exports.pushToQueue = function (uuid, focus, cb) {
    if ( focus === true ) {
        this._connection.rpush(this._getPrefixforProtocol() + ':queue', uuid, cb);
    }else{
        this._connection.lpush(this._getPrefixforProtocol() + ':queue', uuid, cb || focus);
    }
}

exports.popFromQueue = function () {
    var priority, cb;
    if ( typeof arguments[0] === 'function' ) {
        cb = arguments[0];
    }else{
        priority = arguments[0];
        cb = arguments[1];
    }

    if ( !util.isArray(priority) ) {
        priority = (priority && [priority]) || [];
    }

    priority = priority.concat(['high', 'med', 'low']).map( function (item) {
        return this._getPrefixforProtocol() + ':buffer:unrestricted:' + item;
    }.bind(this));

    priority.unshift(this._getPrefixforProtocol() + ':queue');
    priority.push(1);

    this._connection.brpop(priority, function (err, reply) {
        var uuid = reply && reply[1];
        var from = reply && reply[0] && reply[0].split(':')[5];
        cb && cb(err, uuid, from);
    });
}

exports.lenOfQueue = function (cb) {
    this._connection.llen(this._getPrefixforProtocol() + ':queue', cb);
}

// Completed

exports.pushToCompleted = function (uuid, cb) {
    this._connection.rpush(this._getPrefixforProtocol() + ':completed', uuid, cb);
}

exports.popFromCompleted = function (cb) {
    this._connection.brpop(this._getPrefixforProtocol() + ':completed', 1, function (err, reply) {
        cb && cb(err, reply && reply[1]);
    });
}

exports.lenOfCompleted = function (cb) {
    this._connection.llen(this._getPrefixforProtocol() + ':completed', cb);
}

// Failed

exports.pushToFailed = function (uuid, cb) {
    this._connection.rpush(this._getPrefixforProtocol() + ':failed', uuid, cb);
}

exports.popFromFailed = function (cb) {
    this._connection.brpop(this._getPrefixforProtocol() + ':failed', 1, function (err, reply) {
        cb && cb(err, reply && reply[1]);
    });
}

exports.lenOfFailed = function (cb) {
    this._connection.llen(this._getPrefixforProtocol() + ':failed', cb);
}

// Processing

exports.pushToProcessing = function (uuid, cb) {
    this._connection.rpush(this._getPrefixforProtocol() + ':processing', uuid, cb);
}

exports.rmUuidOfProcessing = function (uuid, cb) {
    this._connection.lrem(this._getPrefixforProtocol() + ':processing', 0, uuid, cb);
}

exports.existsUuidInProcessing = function (uuid, cb) {
    this._connection.lrange(this._getPrefixforProtocol() + ':processing', -1000, 1000, function (err, reply){
        cb(err, (reply && reply.indexOf(uuid) > -1) || false);
    });
}

exports.lenOfProcessing = function (cb) {
    this._connection.llen(this._getPrefixforProtocol() + ':processing', cb);
}

// Job

exports.setJob = function (uuid, obj, cb) {
    var hashes = [this._getPrefix() + ':job:' + uuid];
    for(var key in obj){ 
        hashes.push(key);
        hashes.push((key == 'data' ? JSON.stringify(obj[key]) : obj[key]));
    }
    this._connection.hmset( hashes, cb);
}

exports.getJob = function (uuid, cb) {
    this._connection.hgetall(this._getPrefix() + ':job:' + uuid, function(err, reply){
        if ( reply && reply['data'] ) {
            reply['data'] = JSON.parse(reply['data']);
        }
        cb(err, reply);
    });
}

exports.delJob = function (uuid, cb) {
    var keys = [ ':job:' + uuid, ':job:' + uuid + ':timeout'];
    async.each( keys, function (item, cb) {
        this._connection.del( this._getPrefix() + item, cb);
    }.bind(this), function (err) {
        cb && cb(err);
        delete key;
    });
}

exports.cleanJob = function (uuid, cb) {
    var queue = [ ':queue', ':completed', ':failed', ':buffer:' + this.protectKey + ':high', ':buffer:' + this.protectKey + ':med', ':buffer:' + this.protectKey + ':low' ];
    async.map( queue, function (item, cb) {
        this._connection.lrem(this._getPrefixforProtocol() + item, 0, uuid, function (err, reply) {
            cb(null, (err && 0) || reply || 0);
        });
    }.bind(this), function (err, result) {
        var count = 0;
        for (var i = result.length - 1; i > -1; i-= 1) {
            count += result[i];
        };
        cb && cb(err, count);
        delete queue;
    });
}

exports.expireJob = function (uuid, cb) {
    this._connection.expire( this._getPrefix() + ':job:' + uuid, 3 * 24 * 60 * 60, cb);
}

exports.setTimeoutOfJob = function (uuid, timeout, cb) {
    async.series([
        function (cb) {
            this._connection.set( this._getPrefix() + ':job:' + uuid + ':timeout', 1, cb);
        }.bind(this),
        function (cb) {
            this._connection.expire( this._getPrefix() + ':job:' + uuid + ':timeout', timeout, cb);
        }.bind(this)
    ], function (err) {
        cb && cb(err);
    });
},

// Buffer

exports.pushToBufferByProtect = function (protectKey, uuid) {
    var priority, cb;
    if ( typeof arguments[2] === 'function' ) {
        cb = arguments[2];
    }else{
        priority = arguments[2];
        cb = arguments[3];
    }
    priority = priority || 'med';
    this._connection.lpush(this._getPrefixforProtocol() + ':buffer:' + protectKey + ':' + priority , uuid, cb);
}

exports.popFromBufferByProtect = function (protectKey) {
    var priority, cb;
    if ( typeof arguments[1] === 'function' ) {
        cb = arguments[1];
    }else{
        priority = arguments[1];
        cb = arguments[2];
    }

    if ( !util.isArray(priority) ) {
        priority = (priority && [priority]) || [];
    }

    priority = priority.concat(['high', 'med', 'low']).map( function (item) {
        return this._getPrefixforProtocol() + ':buffer:' + protectKey +':' + item;
    }.bind(this));

    priority.push(1);

    this._connection.brpop(priority, function (err, reply) {
        var uuid = reply && reply[1];
        var from = reply && reply[0] && reply[0].split(':')[5];
        cb && cb(err, uuid, from);
    });
}

exports.fetchProtectFromBuffer = function (cb) {
    this._connection.keys(this._getPrefixforProtocol() + ':buffer:*', function(err, reply){
        var item, protectKey = [];
        if ( err === null && reply){
            for (var i = 0, length = reply.length; i < length; i+=1) {
                item = reply[i].split(':')[4];
                if ( item && protectKey.indexOf(item) < 0 ) {
                    protectKey.push(item);
                }
            }
        }

        cb(err, protectKey);

        delete reply;
        delete protectKey;
    });
}

exports.incrementWorkload = function (protectKey, cb) {
    this._connection.zincrby( this._getPrefixforProtocol() + ':workload', '1', protectKey, cb);
}

exports.decrementWorkload = function (protectKey, cb) {
    this._connection.zincrby( this._getPrefixforProtocol() + ':workload', '-1', protectKey, cb);
}

exports.fetchOverWorkload = function (max, cb) {
    max = max || 5;
    var protect = {}
    this._connection.zremrangebyscore( this._getPrefixforProtocol() + ':workload', '-inf', 0, function () {
        this._connection.zrangebyscore( this._getPrefixforProtocol() + ':workload', max, '+inf', 'WITHSCORES', function (err, reply) {
            if ( err == null && reply){
                for (var i = 0, length = reply.length; i < length; i+=2) {
                    protect[reply[i]] = reply[i+1];
                };
            }
            cb(err, protect);
        });
    }.bind(this));
}

// Schedule

exports.pushToSchedule = function (timestamp, uuid, cb) {
    this._connection.lpush( this._getPrefixforProtocol() + ':schedule:' + timestamp, uuid, cb);
}

exports.fetchScheduleByTimestamp = function (timestamp, cb) {
    var keys = [];
    this._connection.keys( this._getPrefixforProtocol() + ':schedule:*', function (err, reply) {
        if ( err == null && reply ) {
            for (var i = 0, length = reply.length; i < length; i+=1) {
                if ( (reply[i] && reply[i].split(':')[4]) <= timestamp ) {
                    keys.push(reply[i]);
                }
            };
        }
        cb(err, keys);
    });
}

exports.popUuidFromSchedule = function (timestamp, cb) {
    this._connection.rpop( this._getPrefixforProtocol() + ':schedule:' + timestamp, cb);
}

// Other
exports.getTime = function (cb) {
    this._connection.time(function (err, reply) {
        cb(err, reply && reply[0]);
    });
}