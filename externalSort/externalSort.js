var bufferSize=10;
var lineCounter=0;
var tmpFileName='externalSort.tmp';
var fs = require('fs');

module.exports= {
externalSort: function (inputStream, compareFunc) {
    inputStream.on('data',function(row) { return handleInputRow(row,compareFunc); });
}
};

function handleInputRow(row,compareFunc)  {
    arguments.callee.rowCount = arguments.callee.rowCount + 1 || 1; 
    arguments.callee.buffer = arguments.callee.buffer || []; // initialize empty array
    arguments.callee.buffer.push(row); 
    if (arguments.callee.rowCount==bufferSize) // we read the maximum number of lines we want in memory
    {
        arguments.callee.fileCount = arguments.callee.rowCount + 1 || 0;
        arguments.callee.rowCount=0; // reset count
        arguments.callee.buffer.sort(compareFunc)
        let padToFour = number => number <= 9999 ? ("000"+number).slice(-4) : number;
        var fileName = tmpFileName + padToFour(arguments.callee.fileCount);
        fs.writeFile(fileName,JSON.stringify(fileName,arguments.callee.buffer));
    }
}

function handleInputEvent(compareFunc) {
    return function(row) { return handleInputRow(row,compareFunc); }
}


fs.writeFile("/tmp/test", "Hey there!", function(err) {
    if(err) {
        return console.log(err);
    }

    console.log("The file was saved!");
}); 