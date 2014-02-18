module.exports = function (list, value) {
    var count = 0;
    for (var i = 0; i < list.length; i++) {
        if ( list[i] === value ) {
            count += 1;
        };
    };
    return count;
}
