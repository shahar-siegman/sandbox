const fs = require('fs')
const fastCsv = require('fast-csv')
const through = require('through')
const mergeStream = require('merge-stream')
const multiPipe = require('multipipe')
const compareUtils = require('../comparer/comparer')
const BatchStream = require('batch-stream2')

function storeToFiles(options) {
    options = Object.assign({ size: 100 }, options);
    var compare = options.compare || compareUtils.objectComparison(options.fieldnames),
        i = 0,
        isDone = false,
        main

    // objects -> (batch) -> arrays -> (sorter-storer) -> files -> (multiunion) -> objects 
    var batch = new BatchStream({ size: options.size }),
        storeAndSort = through(storeNextBatch, function () { isDone = true; console.log('storeAndSort finish'); }),
        reader = mergeReader(compare);

    return multiPipe(batch, storeAndSort, reader);
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


function mergeReader(comp) {
    var streamHeads = {},
        streamsArray = [],
        numInputStreams = 0,
        allFileNamesArrived = false,
        readable = mergeStream(),
        numActiveStreams;
    function onData(currFileName) {
        console.log('file: ' + currFileName)
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
                this.queue(null) // when debugging: make sure streamHeads is empty at this point.
        })
    }

    function checkIfHeadArrayReady(outputStream, index) {
        if (Object.keys(streamHeads).length == numActiveStreams) {
            var elementsToPush = findSmallestElementOrElements(streamHeads, comp);
            for (index in elementsToPush) {
                element = elementsToPush[index];
                outputStream.queue(element);
                streamsArray[Index].resume();
            }
        }
    }


    function findSmallestElementOrElements(map, comp) {
        // split a map with smallest element or elements as the return value. these elemets are subtracted from the original
        var smallestValue,
            ret = {};
        for (key in map) {
            var value = map[key];
            if (!smallestValue)
                smallestValue = value;
            switch (comp(value, smallestValue)) {
                case -1:
                    smallestValue = value;
                    ret = { key: value };
                    break;
                case 0:
                    ret[key] = value;
                    break;
                default:
            }
            for (var retKey of Object.keys(ret))
                map[retKey] = undefined;
            return ret
        }
    }
}

function fileNameByNum(num) {
    y = num.toString().length;
    return 'tmp/tmp' + '0'.repeat(Math.max(0, 5 - y)) + num + '.csv';
}
module.exports = storeToFiles;
