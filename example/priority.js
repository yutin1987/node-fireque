var Fireque = require('../index.js');
var redis = require("redis"),
    client = redis.createClient(),
    charm = require('charm')();

charm.pipe(process.stdout);
charm.reset();

var worker = ['off','off','off','off','off','off','off','off','off','off'];
var jobs = [];
var workload = 0;
var queue = 0;

client.flushall(function () {
    /**
     * Example Code
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
            }, 500);
        });
    });

    // Consumer
    var consumer = new Fireque.Consumer('addition', {connection: true});
    consumer.onCompleted( function (completed_jobs, callback) {
        completed_jobs.forEach(function (job) {
            var num = job.data.num;
            var show = job.data.ans + '';
            for (var i = show.length; i < 4; i++) {
                show = '0' + show;
            };
            jobs[num] = show;
        });
        callback();
    }, {max_count: 3});

    // Keeper
    var keeper = new Fireque.Keeper('addition', 3, {connection: true});
    keeper.start(function (err, res){}, 0);

    // Job
    for (var i = 119; i >= 0; i-=1) {
        job = new Fireque.Job('addition', {x: i, y: i, num: i});
        job.enqueue(i < 40 ? 'high' : i < 80 ? 'med' : 'low' ,{protectKey: 'key'});
    };

    // Example End
});

setInterval(function () {
    var obj = new Fireque.Job('addition');
    client.zscore( obj._getPrefixforProtocol() + ':workload', 'key', function (err, reply) {
        if (err == null){
            workload = reply || 0;
        }
    });

    charm.position(0, 2);
    charm.foreground('white').write('Worker: ');
    for (var i = 0; i < 10; i++) {
        charm.move(1, 0)
             .foreground(worker[i] == 'off' ? 'green' : 'cyan')
             .write(worker[i]);
    }
    charm.position(0, 3);
    charm.foreground('white').write('Workload: ' + workload);
    charm.position(0, 4);
    charm.foreground('white').write('Job:');
    var priority = ['High', 'Med', 'Low'];
    var color = ['red', 'magenta', 'yellow'];
    for (var p = 0; p < 3; p++) {
        charm.position(0, 5 + p * 5);
        charm.foreground('white').write(' - ' + priority[p] + ' - ');
        for (var i = 0; i < 4; i++) {
            charm.position(0, 5 + i + p * 5 + 1);
            for (var j = 0; j < 10; j++) {
                charm.move(3, 0)
                     .foreground(jobs[j + i * 10 + p * 40] !== undefined ? 'black' : color[p])
                     .write(jobs[j + i * 10 + p * 40] !== undefined ? '✔' : '◎' );
            }
        }
    };
    charm.position(0, 0);

}, 150);