var Fireque = require('../index.js');

var worker = new Fireque.Worker('addition');
worker.onPerform( function (job, callback) {
    job.data.ans = job.data.x + job.data.y;
    callback(false);
});

var job = new Fireque.Job('addition', {x: 1, y: 1});
job.enqueue();

producer = new Fireque.Producer('addition');

producer.onCompleted( function (jobs, callback) {
    var x = jobs[0].data.x;
    var y = jobs[0].data.y;
    var ans = jobs[0].data.ans; 
    console.log(x + '+' + y + '=' + ans);
    callback();
}, {max_count: 1});