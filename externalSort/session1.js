const BatchStream = require('batch-stream');
const stringifyStream = require('stringify-stream')
const Readable = require('stream').Readable;
var through = require('through');
var myReadable= new Readable({objectMode:true});
var fs = require('fs')
var lbl = require('linebyline')

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

function handleIncomingBatch(dataBatch,comp,counter) {
    y=counter.toString().length;
    var filename='tmp'+'0'.repeat(Math.max(0,5-y))+counter+'.csv';
    dataBatch.sort(comp);
    fs.writeFileSync(filename, JSON.stringify(dataBatch));
}

var testSort ={};
testSort.test = function() 
{
    myReadable.pipe(batch).pipe()
}

function sort(comp)
{
    return through(
        function(dataBatch) {
            this.i = this.i || 0;
            handleIncomingBatch(dataBatch,comp,this.i);
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
testSort.test = function() 
{
    var mySortTranfrom=sort(myCompare);
    myReadable.pipe(batch).pipe(mySortTranfrom);
}

testSort.test();