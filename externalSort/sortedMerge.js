fastCsv = require('fast-csv')

function sortedMerge() {
    var ind=0,
    inputStreamArray = [],
    objectBuffer =[];
    inputsAreAllHere = false;
    function onNewFile(filename) {
        inputStreamArray.push({readable: filenameToStream(filename), isReady: false})
        inputStreamArray.on('data', dataHandler(ind, this)) // dataHandler(ind, this) creates a function that emits sorted objects from array location ind
        ind++;
    }

    function dataHandler(i, self) {
        return function(data) {
            this.pause();
            objecBuffer[ind]={data: data, isPopulated: true};
            objectBuffer.emit('data', i)
        }
    }
    objectBuffer.on('data', function(i) {
        if (inputsAreAllHere && objectBuffer.every(x => x.isPopulated))
            pushSmallestElementFromBuffer();
    })
}