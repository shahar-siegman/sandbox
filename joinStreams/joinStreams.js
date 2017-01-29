const csvjson = require('csvjson');
const csv2array = require('./csv2array');
const fs = require('fs');
var streamify = require('stream-array');
const readable = require('stream').Readable;

module.exports=
function joinStreams(streamA, streamB, comp, joinType, fuseFunc, emptyA, emptyB) {
    // streamA and streamB must already be sorted
    var keepAs = false, keepBs = false;
    switch (joinType.toLowerCase()) {
        case 'left':
            keepAs = true;
            break;
        case 'right':
            keepBs = true;
            break;
        case 'outer':
        case 'full-outer':
        case 'full_outer':
        case 'fullouter':
            keepAs = true;
            keepBs = true;
            break;
    }

    var isDoneA = false, isDoneB = false;
    var objA, objB;

    var joinReadable = new readable();
    joinReadable._read = joinLoop;
    return joinReadable;

    function joinLoop() {
        var toPush;
        do {
            objA || isDoneA || (objA = streamA.read());
            objA || (isDoneA = true);

            objB || isDoneB || (objB = streamB.read());
            objB || (isDoneB = true);
            if (isDoneA || isDoneB)
                toPush = finishLoop(this)
            else
                switch (comp(objA, objB)) {
                    case -1: // A < B
                        if (keepAs)
                            toPush = fuse(objA, emptyB);
                        objA = undefined;
                        break;
                    case 0: // A matches B
                        toPush = fuse(objA, objB);
                        objA = undefined;
                        objB = undefined;
                        break;
                    case 1: // A > B
                        if (keepBs)
                            toPush = fuse(emptyA, objB);
                        objB = undefined;
                        break;
                }
        } while (typeof toPush == 'undefined')
        this.push(toPush);

        function finishLoop() {
            var toPush;
            if (isDoneB && !isDoneA && keepAs) { // B stream depleted
                toPush = fuse(objA, emptyB);
                objA = undefined;
            }
            else if (isDoneA && !isDoneB && keepBs) { // A stream depleted
                toPush = fuse(emptyA, objB);
                objB = undefined;
            }
            else // completed processing
                toPush = null;
            return toPush;
        }
    }

}

test1 = function () {
    var emptyA = { id: 0, account: "", domain: "" };
    var emptyB = { id: 0, account: "", manager: "" };

    var streamA = csv2array.CSVfileReader('sampleInput1.csv', emptyA);
    var streamB = csv2array.CSVfileReader('sampleInput2.csv', emptyB);
    var outStream = fs.createWriteStream('testOutput.json');

    streamA.on('close', function () { console.log('Stream A closed!') })
    streamB.on('close', function () { console.log('Stream B closed!') })

    comp = function (a, b) { return a.account < b.account ? -1 : a.account == b.account ? 0 : 1; }
    fuse = function (a, b) { var a = csvjson.toCSV({ id: a.id, account: a.account, domain: a.domain, manager: b.manager }, { headers: "none" }); console.log(a); return (a); };

    joinStreams(streamA, streamB, comp, 'right', fuse, emptyA, emptyB).pipe(outStream);

}


