const fastCsv = require('fast-csv');
const Readable = require('stream').Readable;
const through = require('through');
const BatchStream = require('batch-stream2');
const multiUnion = require('./sortedUnionMultiStream.js');
const from = require('from2-array')
const compareUtils = require('./comparer.js')
const fs = require('fs')

// options
// compare: a compare function (takes two arguments and returns either -1, 0, or 1, with the conditions compare(a,a)==0 and compare(a,b)==-compare(b,a))
// fieldnames: instead of specifying the compare function, you can specify the name of the fields to be compared as an array
function externalSorter(options) {
    options = Object.assign({ size: 100 }, options);
    var i = 0, j = 0, a = [], compare = options.compare || compareUtils.objectComparison(options.fieldnames)

    var batch = new BatchStream(
        {
            size: options.size,
            transform:
            function storeNextBatch(items, callback) {
                 console.log('transform. a before '+ JSON.stringify(a))
                var currFileName = fileNameByNum(i);
                fastCsv.writeToPath(currFileName, items.sort(compare), { headers: true }).on("finish", function () {console.log("tranform finish " + i); callback() })
                a.push(currFileName);
                console.log('a after '+ JSON.stringify(a))
                i++;
            }
        })

    return through(function (data) { batch.write(data) },
        function () {
            console.log('end fired')
            var b = [], data;
            console.log('a is ' + JSON.stringify(a));
            for (var v of a) { console.log('v is ' + v); b.push(fastCsv.fromPath(v)); }
            outS = multiUnion(b, compare);
            do {
                data = outS.read();
                console.log('data is ' + JSON.stringify(data))
                this.queue(data);
            } while (data)
        })
}


function fileNameByNum(num) {
    y = num.toString().length;
    return 'tmp/tmp' + '0'.repeat(Math.max(0, 5 - y)) + num + '.csv';
}

function myCompare(x1,x2)
{
    return x1.a > x2.a ? 1 : x1.a==x2.a ? 0 : -1;
}

function testExtSort() {
    var myReadable = new Readable({ objectMode: true });
    myReadable._read = function () {
        if (!this.i)
            this.i = 0;
        this.i++;
        if (this.i < 50)
            this.push({ a: Math.round(1000 * Math.random()), b: this.i });
        else
            this.push(null)
    }
    var toCSV = fastCsv.createWriteStream({headers: true}),
    outFile = fs.createWriteStream("result.csv");
    myReadable.pipe(through(function(data) { this.queue(JSON.stringify(data)+', ') } )).pipe(process.stdout)
    myReadable.pipe(externalSorter({size: 15, fieldnames: ["a"] })).pipe(toCSV).pipe(outFile)
}

// options…}

testExtSort()