var async = require("async");
async.series([
    function (cb){
        cb(null);
    },
    function (cb){
        cb(123);
    },
    function (cb){
        cb(null);
    }
], function (err, r) {
    console.log(err, r.length);
});