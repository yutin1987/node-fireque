var uuid = require('node-uuid');
var redis = require("redis");
var job = require('./src/job.js');
var worker = require('./src/work.js');
var producer = require('./src/producer.js');
var monitor = require('./src/monitor.js');

var tasks = {};

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

// var getTimestamp = function(){
//   return new Date().getTime();
// }

// module.exports = {
//   create: function(protocol, data){
//     if( !protocol ){
//       console.log('ERROR: Protocol is required.');
//       return false;
//     }else{
//       var uid = uuid.v4();
//       task = new Task(protocol, uid, data);
//       return task;
//     }
//   },
//   getQueue: function(protocol, callback, timeout){
//     if ( timeout === undefined ) {
//       timeout = 0;
//     }
//     studio.brpoplpush('queue_'+protocol, 'processing_'+protocol, timeout, function(err, uid){
//       if ( err === null && uid){
//         new Task(protocol, uid, function(task){
//           callback(err, task);
//         });
//       }else{
//         callback(err, false);
//       }
//     });
//   },
//   getCompleted: function(protocol, callback, timeout){
//     if ( timeout === undefined ) {
//       timeout = 0;
//     }
//     studio.brpop('completed_'+protocol, timeout, function(err, reply){
//       if ( err === null && reply ) {
//         new Task(protocol, reply[1], function(task){
//           callback(err, task);
//         });
//       }else{
//         callback(err, false);
//       }
//     });
//   },
//   getTimeout: function(protocol, callback, timeout){
//     if ( timeout === undefined ) {
//       timeout = 30;
//     }
//     var dead = [];
//     var wait = 0;
//     var length = tasks.length;
//     studio.lrange('queue_' + protocol, -100, 100, function( err, queue ){
//       var length = queue.length;
//       for (var i = 0; i < length; i++) {
//         if ( tasks[queue[i]] === undefined ) {
//           tasks[queue[i]] = new Date().getTime();
//         }else if ( (new Date().getTime() - tasks[queue[i]]) > (timeout * 1000)){
//            dead.push(new Task(protocol, queue[i], function(){ wait -= 1;}));
//         }
//       };
//       while( wait > dead.length);
//       callback(err, dead);
//     });
//   },
//   exit: function () {
//     studio.end();
//   }
// }

// var Task = (function(){
//   var constructor = function(protocol, uid, data){
//     var self = this;
//     this.protocol = protocol;
//     this.uid = uid;
//     if (data && typeof data === 'function'){
//         var callback = data;
//         studio.get( 'task_'+self.uid, function(err, data) {
//           if ( err === null ){
//             self.data = JSON.parse(data);
//             callback(self);
//           }else{
//             console.log('ERROR: get task data false by id: ' + self.uid);
//           }
//         });
//     }else{
//       this.data = data;
//     }
//   }

//   constructor.prototype = {
//     uid: null,
//     data: null,
//     protocol: null,
//     assign: function(callback){
//       var self = this;
//       studio.set( 'task_'+self.uid, JSON.stringify(self.data), function(err, reply) {
//         if ( err === null ){
//           studio.lpush( 'queue_'+self.protocol, self.uid, function(err, reply) {
//             if ( err === null ){
//               callback(err, self);
//             }else{
//               console.log(err);
//             }
//           });
//         }else{
//           console.log(err);
//         }
//       });
//     },
//     completed: function(data, callback){
//       var self = this;
//       studio.set( 'task_'+self.uid, JSON.stringify(self.data), function(err, reply) {
//         if ( err === null ){
//           studio.lrem( 'processing_'+self.protocol, 0, self.uid);
//           studio.lpush( 'completed_'+self.protocol, self.uid)
//           callback(err, reply);
//         }
//       });
//     },
//     retry: function(callback){
//       var self = this;
//       studio.set( 'task_'+self.uid, JSON.stringify(self.data), function(err, reply) {
//         if ( err === null ){
//           studio.lrem( 'processing_'+self.protocol, 0, self.uid);
//           studio.lpush( 'queue_'+self.protocol, self.uid)
//           callback(err, reply);
//         }
//       });
//     }
//   }

//   return constructor;
// })();