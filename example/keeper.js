var Fireque = require('../index.js');
var redis = require("redis"),
    client = redis.createClient(),
    charm = require('charm')();

// ./fireque keeper -p addition -w 6

charm.pipe(process.stdout);
charm.reset();

var worker = ['off','off','off','off','off','off','off','off','off','off'];

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
    [0,1,2,3,4,5,6,7,8,9].forEach(function (index) {
        worker_self = new Fireque.Worker('addition', {connection: true});
        worker_self.onPerform( function (job, callback) {
            worker[index] = 'on ';
            job.data.ans = job.data.x + job.data.y;
            setTimeout(function (){
                worker[index] = 'off';
                callback(false);
            }, 100);
        });
    });

    // Consumer
    var consumer = new Fireque.Consumer('addition', {connection: true});
    consumer.onCompleted( function (completed_jobs, callback) {
        for (var i = 0; i < completed_jobs.length; i++) {
            jobs[completed_jobs[i].data.num] = completed_jobs[i].data.ans;
        };
        callback();
    }, {max_count: 10});


    var protectKey = ['xxx','xxx','xxx','yyy','yyy','yyy','zzz','zzz','zzz'];

    // Job
    for (var i = 0; i < 10; i++) {
        for (var j = 0; j < 60; j++) {
            var job = new Fireque.Job('addition', {x: i, y: i, num: i * 60 + j});
            job.enqueueAt(i * 5 + 4, {protectKey: protectKey[j % 9], priority: j % 3 - 1});
        };
    };
});


var startTime = new Date().getTime();
setInterval(function () {

    charm.position(0, 2);
    charm.foreground('white').write('Worker: ');
    for (var i = 0; i < 10; i++) {
        charm.move(1, 0)
             .foreground(worker[i] == 'off' ? 'green' : 'cyan')
             .write(worker[i]);
    }

    charm.position(0, 4);
    var timeline = Math.floor((new Date().getTime() - startTime) / 1000);
    if ( timeline < 53 ) {
        charm.position(0, timeline + 3);
        charm.foreground('white').write('          ').move(-10,1).write(toTime( startTime / 1000 + timeline) + '➤ ');
    }else{
        charm.position(0, 53 + 4);
        charm.foreground('white').write('END     ☃ ');
    }
    for (var i = 0; i < 10; i++) {
        charm.position(13, i * 5 + 4 + 4);
        charm.foreground('white').write(toTime( startTime / 1000 + i * 5 + 4));
    };
    for (var i = 0; i < 10; i++) {
        charm.position(23, i * 5 + 4 + 4);
        var completed = 0;
        for (var j = 0; j < 60; j++) {
            if ( jobs[i*60+j] !== undefined ) {
                completed += 1;
            }
        };
        completed = ( completed < 10 ? '0' : '') + completed;
        charm.foreground('white').write(' ' + completed + ' / 60');
    };
    charm.position(0, 0);
}, 150);