const fs = require('fs')
const fastCsv = require('fast-csv')
const through = require('through')
const mergeStream = require('merge-stream')

function storeToFiles(options) {
    options = Object.assign({ size: 100 }, options);
    var compare = options.compare || compareUtils.objectComparison(options.fieldnames),
        i = 0,
        isDone = false,
        main

    // objects -> (batch) -> arrays -> (sorter-storer) -> files -> (multiunion) -> objects 
    var batch = new BatchStream({ size: options.size })

    batch.pipe(through(storeNextBatch, function () { isDone = true; console.log('storeAndSort finish') }))

    return batch;
    function storeNextBatch(items) {
        console.log('storeNextBatch ' + i);
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
}


function mergeReader() {
    var streamHeads = [],
        streamsArray = [],
        numInputStreams = 0,
        allFileNamesArrived = false,
        readable = mergeStream();
    numActiveStreams
    function onData(currFileName) {
        var newReadStream = fs.createReadStream(currFileName)
            .pipe(fastCsv.parse({ headers: true }))
            .pipe(streamHeadHandler(numInputStreams));
        newReadStream.pause();
        streamsArray.push(newReadStream);
        readable.add(newReadStream);
        numInputStreams++;
    }

    function onFinish() {
        allFileNamesArrived = true;
        numActiveStreams = numInputStreams;
        streamsArray.forEach(function (stream) { stream.resume(); })
        this.queue(null);
    }

    return through(onData, onFinish);

    function streamHeadHandler(index) {
        return through(function (data) {
            streamHeads[index] = data;
            this.pause();
            checkIfHeadArrayReady(this, index)
        }, function () {
            numActiveStreams--;
            if (numActiveStreams == 0)

        }
    }

    function checkIfHeadArrayReady(outputStream, index) {
        if (streamHeads.length == numInputStreams && streamHeads.every(x => x)) {
            var pushedIndexes = pushSmallestElementOrElements(outputStream, streamHeads, comp);
            pushIndexes.length && pushedIndexes.forEach(index => streamsArray[Index].resume());
        }
    }
}


function pushSmallestElementOrElements(stream, arr, comp) {
    var smallestValue, smallestValueIndex,
        smallestValues = [], smallestValuesIndex = [];
    arr.forEach(function (value, index) {
        if (!smallestValue || smallestValue && comp(value, smallestValue < 0)) {
            smallestValue = value;
            smallestValueIndex = index;
        }
    })
    arr.forEach(function (value, index) {
        if (comp(value, smallestValue) == 0) {
            smallestValues.push(value);
            smallestValuesIndex.push(index);
        }
    })
    if (smallestValue) {
        stream.queue(smallestValue)
        return smallestValueIndex;
    }
}
