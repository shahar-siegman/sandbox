readable = require('stream').Readable
through = require('through')
external = require('./externalSort2')

function longStream(len) {
    var remaining = len,
        ret = new readable({ objectMode: true });
    ret._read = function () {
        var sinkReady = true;
        while (sinkReady && remaining) {
            var k = Math.floor(Math.random() * len * 10);
            sinkReady = this.push({ a: k })
            remaining--;
        }
        if (!remaining) this.push(null)
    }
    return ret
}

longStream(100).pipe(external({size:20, fieldnames: 'a'}))
    .pipe(through(function (data) { this.queue(JSON.stringify(data)) }))
    .pipe(process.stdout)