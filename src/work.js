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
        perform: function(action) {
            var self = this;
            var workload = self.workload;
            var queueName = Fireque._getQueueName();
            var nextJob = function(index){
                if ( workload > 0 ) {
                    doPerform(index + 1);
                }else{
                    self.exit();
                }
            }
            var doPerform = null;
            (doPerform = function (index) {
                if ( index >= self._priority.length ) {
                    index = 0;
                }
                var priority = self._priority[index];
                self._connection.brpoplpush(queueName + ':' + self.protocol + ':queue:' + priority, queueName + ':' + self.protocol + ':processing', self._wait, function(err, reply){
                    if ( err === null && reply ) {
                        new Fireque.Job(reply, function(err, job){
                            workload -= 1;
                            try{
                                if ( err !== null ) {
                                    throw (new Error('get Job data error.'))
                                }

                                action(job, function(status){
                                    if ( status === true ) {
                                        job.completed();
                                    }else if ( status === false ) {
                                        job.failed();
                                    }
                                    nextJob(index);
                                });
                            }catch(e){
                                // e.message || e
                                job.failed();
                                nextJob(index);
                            }
                        }, {
                            connection: self._connection
                        });
                    }else{
                        nextJob(index);
                    }
                });
            })(0);
        },
        exit: function () {

        }
    }

    return constructor;

})();