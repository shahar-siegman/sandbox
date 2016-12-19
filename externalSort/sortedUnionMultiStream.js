var union = require('./sorted-union-stream')
var from = require('from2-array')
var through = require('through')


function multiUnion(arrayOfStreams,compare) {
    var l = arrayOfStreams.length;
    console.log('l= '+l)
    switch (l) {
        case 0:
            return from.obj([]); // a stream with no data
        case 1:
            return arrayOfStreams[0] // the only entry is a stream
        case 2:
            return union(arrayOfStreams[0], arrayOfStreams[1],compare)
        default:
            var halfOfL = Math.floor(l / 2);
            var h1 = multiUnion(arrayOfStreams.slice(0, halfOfL),compare)
            h2 = multiUnion(arrayOfStreams.slice(halfOfL, l),compare)
            return union(h1,h2,compare)
    }
}

function testMU() {
    var sorted1 = from.obj([1, 10, 24, 42, 43, 50, 55])
    var sorted2 = from.obj([10, 42, 53, 55, 60])
    var sorted3 = from.obj([11, 14, 24, 50, 52, 53])
    var sorted4 = from.obj([2, 6, 12, 25])
    var sorted5 = from.obj([14, 14, 14])
    var sorted6 = from.obj([])
    var sorted7 = from.obj([])
    var sorted8 = from.obj([])
    var sorted9 = from.obj([])

    var a = multiUnion([sorted1,
        sorted2,
        sorted3,
        sorted4,
        sorted5])
    
    t= through(function(data) { this.queue(data+', ')} )
    a.pipe(t).pipe(process.stdout)
}

module.exports = multiUnion;

