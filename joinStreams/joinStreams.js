const csvjson = require('csvjson');
const fs = require('fs');
var streamify = require('stream-array');
const readable = require('stream').Readable;


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
        objA || isDoneA || (objA = streamA.read());
        objA || (isDoneA = true);

        objB || isDoneB || (objB = streamB.read());
        objB || (isDoneB = true);
        if (isDoneA || isDoneB)
            finishLoop(this)
        else
            switch (comp(objA, objB)) {
                case -1: // A < B
                    if (keepAs)
                        this.push(fuse(objA, emptyB)+'\n');
                    objA = undefined;
                    break;
                case 0: // A matches B
                    this.push(fuse(objA, objB)+'\n');
                    objA = undefined;
                    objB = undefined;
                    break;
                case 1: // A > B
                    if (keepBs)
                        this.push(fuse(emptyA, objB)+'\n');
                    objB = undefined;
                    break;
            }

        function finishLoop(me) {
            if (isDoneB && !isDoneA && keepAs) { // B stream depleted
                me.push(fuse(objA, emptyB)+'\n');
            }
            else if (isDoneA && !isDoneB && keepBs) { // A stream depleted
                me.push(fuse(emptyA, objB)+'\n');
            }
            else // completed processing
                me.push(null);
        }
    }

}

test1 = function () {
    var data1 = fs.readFileSync('sampleInput1.csv', { encoding: 'utf8' });
    var data2 = fs.readFileSync('sampleInput2.csv', { encoding: 'utf8' });
    var options = { delimiter: ',' };
    var arrayA = csvjson.toSchemaObject(data1, options);
    var arrayB = csvjson.toSchemaObject(data2, options);
    var streamA = streamify(arrayA);
    var streamB = streamify(arrayB);
    var outStream = fs.createWriteStream('testOutput.json');
    comp = function (a, b) { return a.account < b.account ? -1 : a.account == b.account ? 0 : 1; }
    fuse = function (a, b) { return JSON.stringify({ id: a.id, account: a.account, domain: a.domain, manager: b.manager }) };
    emptyA = { id: 0, account: "", domain: "" };
    emptyB = { id: 0, account: "", manager: "" };


    joinStreams(streamA, streamB, comp, 'inner', fuse, emptyA, emptyB).pipe(outStream);

}

test1();
