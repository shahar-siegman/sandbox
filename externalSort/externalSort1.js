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
    var compare = options.compare || compareUtils.objectComparison(options.fieldnames),
    i = 0, 
    isDone=false, 
    main 
    
    // objects -> (batch) -> arrays -> (sorter-storer) -> files -> (multiunion) -> objects 
    var batch = new BatchStream({ size: options.size })

    var sortAndStore = through(storeNextBatch, function () { isDone = true; console.log('storeAndSort finish') })

    function storeNextBatch(items) {
        console.log('transform ' + i);
        var currFileName = fileNameByNum(i);
        main = this;
        i++;
        fastCsv.writeToPath(currFileName, items.sort(compare), { headers: true })
            .on("finish", function () {
                console.log("storing finished " + i);
                main.queue(currFileName);
                i--;                        
                if (isDone && i == 0)
                    main.queue(null)
            })

    }

    var bufferUnion = through(function (data) {
        this.a || (this.a = [])
        console.log('bufferUnion received ' + data);
        this.a.push(data)
    }, function () {
        console.log('multiunion files ' + JSON.stringify(this.a))
        var inS = this.a.map(x => fastCsv.fromPath(x, { headers: true }))
        //fastCsv.fromPath("tmp/tmp00000.csv").pipe(through(function(data) {this.queue(JSON.stringify(data))})).pipe(process.stdout);
        // fs.createReadStream("tmp/tmp00000.csv").pipe(process.stdout);
        var outS = multiUnion(inS, compare);
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
    //myReadable.pipe(through(function (data) { this.queue(JSON.stringify(data) + ', ') })).pipe(process.stdout)
    myReadable.pipe(externalSorter({ size: 15, fieldnames: ["a"] })).pipe(toCSV).pipe(outFile)
}

// options…}
function testFromPath() {
    var a= ["tmp/tmp00000.csv","tmp/tmp00001.csv","tmp/tmp00002.csv","tmp/tmp00003.csv"]
    var b= a.map(x => fastCsv.fromPath(x, { headers: true }))
   // var s=fastCsv.fromPath(b[0], { headers: true })
    b[0].pipe(through(function(data) {this.queue(JSON.stringify(data))})).pipe(process.stdout);
    b[1].pipe(through(function(data) {this.queue(JSON.stringify(data))})).pipe(process.stdout);
}

function testFromPath2() {
    fs.createReadStream("tmp/tmp00000.csv").pipe(process.stdout);
     fs.createReadStream(".gitignore").pipe(process.stdout);
}
testFromPath2()