var job = require('./src/job.js'),
    worker = require('./src/worker.js'),
    producer = require('./src/producer.js'),
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