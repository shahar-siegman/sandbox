const fastCsv = require('fast-csv');
const Readable = require('stream').Readable;
const through = require('through');
var BatchStream = require('batch-stream2');
var union = require('sorted-union-stream')

var batch = new BatchStream(
    { transform: function(items, callback) {  storeNextBatch(items,callback) } } )
 
function storeNextBatch(items,callback) {
    this.i = this.i || 0;
    fastCsv.writeToPath(fileNameByNum(i),items.sort(comp),{headers: true}).on("finish",callback)
}

function readSorted() {

}


docs.pipe(batch)
.on('finish', function() {
  console.log('All doc inserted.')
})