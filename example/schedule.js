var Fireque = require('../index.js');
var redis = require("redis"),
    client = redis.createClient(),
    charm = require('charm')();

charm.pipe(process.stdout);
charm.reset();

function toTime (time) {
    if ( time !== undefined ) {
        return new Date(time * 1000).toLocaleTimeString();
    }else{
        return new Date().toLocaleTimeString();
    }
}

var jobs = [];

client.flushall(function () {
    /**
     *  Example Code
     */

    // Worker
    var worker = new Fireque.Worker('addition', {connection: true});
    worker.onPerform( function (job, callback) {
        job.data.ans = job.data.x + job.data.y;
        setTimeout(function(){
            callback(false);
        }, 200);
    });

    // Consumer
    var consumer = new Fireque.Consumer('addition', {connection: true});
    consumer.onCompleted( function (completed_jobs, callback) {
        for (var i = 0; i < completed_jobs.length; i++) {
            jobs[completed_jobs[i].data.num] = completed_jobs[i].data.ans;
        };
        callback();
    }, {max_count: 1});

    // Keeper
    var keeper = new Fireque.Keeper('addition', 5, {connection: true});
    keeper.start(function (err, res){}, 0);

    // Job
    for (var i = 0; i < 5; i++) {
        for (var j = 0; j < 10; j++) {
            var job = new Fireque.Job('addition', {x: i, y: i, num: i * 10 + j});
            job.enqueueAt(i * 5 + 4);
        };
    };
});


var startTime = new Date().getTime();
setInterval(function () {
    charm.position(0, 2);
    var timeline = Math.floor((new Date().getTime() - startTime) / 1000);
    if ( timeline < 30 ) {
        charm.position(0, timeline + 1);
        charm.foreground('white').write('          ').move(-10,1).write(toTime( startTime / 1000 + timeline) + '➤ ');
    }else{
        charm.position(0, 30 + 2);
        charm.foreground('white').write('END     ☃ ');
    }
    for (var i = 0; i < 5; i++) {
        charm.position(13, i * 5 + 2 + 4);
        charm.foreground('white').write(toTime( startTime / 1000 + i * 5 + 4));
    };
    for (var i = 0; i < 5; i++) {
        charm.position(20, i * 5 + 2 + 4);
        for (var j = 0; j < 10; j++) {
            charm.foreground('white').move(3,0).write(jobs[i*10+j] !== undefined ? '✔' : '◎');
        };
    };
    charm.position(0, 0);
}, 150);