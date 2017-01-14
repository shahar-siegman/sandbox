const fastCsv = require('fast-csv');
const Readable = require('stream').Readable;
const through = require('through');
const BatchStream = require('batch-stream2');
const multiUnion = require('./sortedUnionMultiStream.js');
const from = require('from2-array')
const compareUtils = require('./comparer.js')
const fs = require('fs')
const combine = require('stream-combiner')

// options
// compare: a compare function (takes two arguments and returns either -1, 0, or 1, with the conditions compare(a,a)==0 and compare(a,b)==-compare(b,a))
// fieldnames: instead of specifying the compare function, you can specify the name of the fields to be compared as an array
function externalSorter(options) {
    options = Object.assign({ size: 100 }, options);
    var i = 0, j = 0, main, compare = options.compare || compareUtils.objectComparison(options.fieldnames)

    // objects -> (batch) -> arrays -> (sorter-storer) -> files -> (multiunion) -> objects 
    var batch = new BatchStream({ size: options.size })

    var sortAndStore = through(storeNextBatch, function () { console.log('storeAndSort end') })

    function storeNextBatch(items) {
        console.log('transform ' + i); 
        var currFileName = fileNameByNum(i);
        main =this;
        i++;
        fastCsv.writeToPath(currFileName, items.sort(compare), { headers: true })
            .on("finish", function () {
                main.queue(currFileName)
                console.log("tranform finish " + i);
                i--;
                if (i==0) 
                    main.queue(null)
            })
        
    }

    var bufferUnion = through(function (data) {
        this.a || (this.a = [])
        this.a.push(data)
    }, function () {
        outS = multiUnion(this.a, compare);
        do {
            data = outS.read();
            console.log('data is ' + JSON.stringify(data))
            this.queue(data);
        } while (data)
    }
    )

    return combine([batch, sortAndStore, bufferUnion])
}


function fileNameByNum(num) {
    y = num.toString().length;
    return 'tmp/tmp' + '0'.repeat(Math.max(0, 5 - y)) + num + '.csv';
}

function myCompare(x1, x2) {
    return x1.a > x2.a ? 1 : x1.a == x2.a ? 0 : -1;
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
    var toCSV = fastCsv.createWriteStream({ headers: true }),
        outFile = fs.createWriteStream("result.csv");
    myReadable.pipe(through(function (data) { this.queue(JSON.stringify(data) + ', ') })).pipe(process.stdout)
    myReadable.pipe(externalSorter({ size: 15, fieldnames: ["a"] })).pipe(toCSV).pipe(outFile)
}

// options…}

testExtSort()