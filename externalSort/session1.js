const BatchStream = require('batch-stream');
const stringifyStream = require('stringify-stream')
const Readable = require('stream').Readable;
const through = require('through');
const json2csv = require('json2csv');
const lbl = require('n-readlines')

var myReadable= new Readable({objectMode:true});
var fs = require('fs')


var batch = new BatchStream({ size : 5 });

myReadable._read = function() {
    if (!this.i)
        this.i=0;
    this.i++;
    if (this.i<100)
        this.push({a: Math.round(1000*Math.random()), b: Math.round(10*Math.random())});
    else
        this.push(null)
}

function fnameByNumber(num) {
    y=num.toString().length;
    return 'tmp'+'0'.repeat(Math.max(0,5-y))+num+'.csv';
}

function handleIncomingBatch(dataBatch,comp,counter) {
    var filename=fnameByNumber(counter);
    fs.writeFileSync(filename,'');
    dataBatch.sort(comp);
    for (var i=0; i<dataBatch.length; i++)
        fs.appendFileSync(filename, json2csv({data:dataBatch[i], hasCSVColumnTitle: false}) + '\n');
}

function emitSorted(filenames,comp,fieldnames) {
    var deepComp=function(obj1, obj2) { return comp(obj1.data,obj2.data) };
    var inStreams=[];
    var dataBuffer = []
    // create initial array of objects
    for (var i=0; i < filenames.length; i++)
    {
        inStreams.push(new lbl(filenames[i]))
        dataBuffer.push({source: filenames[i], data: inStreams[i].next().toString('ascii')})
    }
    1;
}


function sort(comp)
{
    return through(
        function(dataBatch) {
            this.i = this.i || 0;
            this.fnames = this.fnames || [];
            var fname = handleIncomingBatch(dataBatch,comp,this.i);
            this.fnames.push(fname);
            this.i++;
        },
        function() {
            console.log('done.');
            this.emit('end');
        }
    )
}

function myCompare(x1,x2)
{
    return x1.a > x2.a ? 1 : x1.a==x2.a ? 0 : -1;
}


var testSort ={};
testSort.test1 = function() 
{
    var mySortTranfrom=sort(myCompare);
    myReadable.pipe(batch).pipe(mySortTranfrom);
}

testSort.test2 = function()
{
    var fnames=[];
    for(i=0; i<20; i++)
        fnames.push(fnameByNumber(i));
    emitSorted(fnames, myCompare, ['a','b']);
}
testSort.test2();