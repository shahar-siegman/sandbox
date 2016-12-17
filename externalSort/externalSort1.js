const fastCsv = require('fast-csv');
const Readable = require('stream').Readable;
const through = require('through');
const BatchStream = require('batch-stream2');
const union = require('./sortedUnionMultiStream.js');
const from = require('from2-array')
const compareUtils = require('./comparer.js')

// options
// compare: a compare function (takes two arguments and returns either -1, 0, or 1, with the conditions compare(a,a)==0 and compare(a,b)==-compare(b,a))
// fieldnames: instead of specifying the compare function, you can specify the name of the fields to be compared as an array
function externalSorter(options) {
    var i = 0, j = 0, a=[], compare = options.compare || compareUtils.objectComparison(options.fieldnames)

    var batch = new BatchStream(
        {
            transform:
            function storeNextBatch(items, callback) {
                var currFileName = fileNameByNum(i);
                fastCsv.writeToPath(currFileName, items.sort(comp), { headers: true }).on("finish", callback)
                a.push(currFileName);
                i++;
            }
        })

    return through(function(data) { batch.write(data) },
    function() {
        b=[];
        for (let v in a) b.push(fastCsv.fromPath(v))
        outS=multiUnion(b, options.compare);
        do {
            data = outS.read();
            this.queue(data);
        } while(data)
    })
}


function fileNameByNum(num) {
    y=num.toString().length;
    return 'tmp'+'0'.repeat(Math.max(0,5-y))+num+'.csv';
}

function testExtSort() {
    var a = [];

}

