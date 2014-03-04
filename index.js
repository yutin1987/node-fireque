var job = require('./src/job.js'),
    worker = require('./src/worker.js'),
    producer = require('./src/producer.js'),
<<<<<<< HEAD
    monitor = require('./src/monitor.js');

module.exports = Fireque = {
  'FIREQUE_HOST': '127.0.0.1',
  'FIREQUE_PORT': '6379',
  'FIREQUE_NAMESPACE': 'noname',
  'Job': job,
  'Worker': worker,
  'Producer': producer,
  'Monitor': monitor,
  _getQueueName: function(){
    return 'fireque:' + this.FIREQUE_NAMESPACE;
  }
}
=======
    keeper = require('./src/keeper.js'),
    model = require('./lib/model.js'),
    util = require('util'),
    redis = require("redis");

module.exports = Fireque = ( function() {
    var fireSelf = {
        host: '127.0.0.1',
        port: 6379,
        databaseIndex: 0,
        namespace: 'noname',
        _connection: null,
        Job: function () {
            return new job(arguments[0], arguments[1], arguments[2], fireSelf);
        },
        Worker: function (protocol, option) {
            return new worker(protocol, option, fireSelf);
        },
        Producer: function (protocol, option) {
            return new producer(protocol, option, fireSelf);
        },
        Keeper: function (protocol, workload, option) {
            return new keeper(protocol, workload, option, fireSelf);
        },
        _createConnection: function (option) {
            var connection = (option && option.connection !== true && option.connection) || redis.createClient(
                (option && option.port) || fireSelf.port ||  6379,
                (option && option.host) || fireSelf.host || '127.0.0.1'
            );

            if ( !(option && option.connection !== true && option.connection) ){
                var databaseIndex = (option && option.databaseIndex) || fireSelf.databaseIndex;
                    databaseIndex && connection.select(databaseIndex);
                if ( fireSelf._connection === null ) {
                    fireSelf._connection = connection;
                }
            }

            return connection;
        },
        _apply: function (obj, option) {
            if ( option && (option.connection || option.port || option.host) ) {
                obj._connection = fireSelf._createConnection(option);
            }else{
                obj._connection = fireSelf._connection || fireSelf._createConnection();
            }
            obj._databaseIndex = fireSelf.databaseIndex;
            obj._getPrefix = function () {
                return 'fireque:' + this._namespace;
            }
            obj._getPrefixforProtocol = function(){
                if ( util.isArray(this.protocol) ) {
                    return this.protocol.concat().map(function(protocol){
                        return this._getPrefix() + ':' + protocol;
                    }.bind(this));
                }else{
                    return this._getPrefix() + ':' + this.protocol;
                }
            }
            obj._namespace = fireSelf.namespace;
            obj._host = fireSelf.host;
            obj._port = fireSelf.port;
            return obj;
        }
    }
    return fireSelf;
})();
>>>>>>> develop_0.5
