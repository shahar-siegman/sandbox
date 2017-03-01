const readable = require('stream').Readable
const through = require('through')
const external = require('./externalSort2')
const fastCsv = require('fast-csv')
const fs = require('fs')
var seed = 1;

function longStream(len) {
    var remaining = len,
        ret = new readable({ objectMode: true });
    ret._read = function () {
        var sinkReady = true;
        while (sinkReady && remaining) {
            var k = Math.floor(random() * len * 10);
            sinkReady = this.push({ a: k })
            remaining--;
        }
        if (!remaining) this.push(null)
    }
    return ret
}


function random() {
    var x = Math.sin(seed++) * 10000;
    return x - Math.floor(x);
}
var a = external({
    size: 20, fieldnames: 'a',
    compare: function (x, y) {
        return parseInt(x.a) < parseInt(y.a) ? -1 : (x.a == y.a ? 0 : 1)
    }})
longStream(30).pipe(a[0]).pipe(a[1]).pipe(a[2])
    .pipe(through(function (data) { console.log('output: '+ JSON.stringify(data)); this.queue(data) }))
    .pipe(fastCsv.createWriteStream({ headers: true }))
    .pipe(fs.createWriteStream('out.csv', 'utf8'))
    .on('finish', function () { console.log('done') })