var job = require('./src/job.js'),
    worker = require('./src/worker.js'),
    producer = require('./src/producer.js'),
    monitor = require('./src/monitor.js'),
    util = require('util'),
    redis = require("redis");

module.exports = Fireque = {
    host: '127.0.0.1',
    port: 6379,
    databaseIndex: 0,
    namespace: 'noname',
    _connection: null,
    Job: job,
    Worker: function (protocol, option) {
        return this._apply(new worker(protocol, option), option);
    },
    Producer: function (protocol, option) {
        return this._appley(new producer(protocol, option), option);
    },
    _createConnection: function (option) {
        var connection = (option && option.connection) || redis.createClient(
            (option && option.port) || this.port ||  6379,
            (option && option.host) || this.host || '127.0.0.1'
        );
        if ( option && !option.connection ){
            var databaseIndex = (option && option.databaseIndex) || this.databaseIndex;
                databaseIndex && connection.select(databaseIndex);
        }
        return connection;
    },
    _apply: function (obj, option) {
        if ( option && (option.connection || option.port || option.host) ) {
            obj._connection = this._createConnection(option);
        }else{
            obj._connection = this._connection || this._createConnection(option);
        }
        obj._databaseIndex = this.databaseIndex;
        obj._getPrefix = function () {
            return 'fireque:' + this._namespace;
        }
        obj._getPrefixforProtocol = function(){
            if ( util.isArray(this.protocol) ) {
                return this.protocol.concat().map(function(protocol){
                    return this._getPrefix() + protocol;
                }.bind(this));
            }else{
                return this._getPrefix() + ':' + this.protocol;
            }
        }
        obj._namespace = this.namespace;
        obj._host = this.host;
        obj._port = this.port;
        return obj;
    }
}