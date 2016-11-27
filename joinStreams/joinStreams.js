const csvjson=require('csvjson');
const fs = require('fs');
var streamify = require('stream-array');
const readable = require('stream').Readable;


function joinStreams(streamA, streamB, outStream, comp, joinType, fuseFunc, emptyA, emptyB) {
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
    //var joinReadable = new readable();
    //joinReadable._read() = joinLoop(undefined,undefined) 
    joinLoop(undefined, undefined)
    return;

    function finishLoop(objA, objB) {
        if (objA) { // B stream depleted
            var nextA = streamA.read();
            outStream.write(fuse(objA, emptyB), finishloop(nextA, undefined));
            return;
        }
        if (objB) { // A stream depleted
            var nextB = streamB.read();
            outStream.write(fuse(emptyA, objB), finishloop(undefined, nextB));
            return;
        }
        // completed processing
        outStream.end();
        1;
    }

    function joinLoop(objA, objB) {
        objA = objA || streamA.read();
        objB = objB || streamB.read();
        if (!objA && objB && keepBs) // A is depleted, write the remaining B's 
            return finishLoop(undefined, objB);
        if (objA && !objB && keepAs) // B is depleted, write the remaining A's
            return finishLoop(objA, undefined);
        if (!objA || !objB) // either is depleted, we may discard
            return finishLoop(undefined, undefined);

        switch (comp(objA, objB)) {
            case -1: // A < B
                if (keepAs)
                    outStream.write(fuse(objA, emptyB), joinLoop(undefined, objB));
                else {
                    while (comp(objA, objB) == -1)
                        objA = streamA.read();
                    joinLoop(objA, objB);
                    return;
                }
                break;
            case 0: // A matches B
                outStream.write(fuse(objA, objB), joinLoop(undefined, undefined));
                break;
            case 1: // A > B
                if (keepBs)
                    outstream.write(fuse(emptyA, objB), joinLoop(objA, undefined));
                else {
                    while (comp(objA, objB) == 1)
                        objB = streamB.read();
                    joinLoop(objA, objB);
                    return;
                }
                break;
        }
    }
}

test1 = function()
{
    var data1 = fs.readFileSync('sampleInput1.csv', { encoding : 'utf8'});
    var data2 = fs.readFileSync('sampleInput2.csv', { encoding : 'utf8'});
    var options = { delimiter : ','};
    var arrayA = csvjson.toSchemaObject(data1, options);
    var arrayB = csvjson.toSchemaObject(data2, options);
    var streamA = streamify(arrayA);
    var streamB = streamify(arrayB);
    var outStream = fs.createWriteStream('testOutput.json');
    comp = function(a,b) {return a.account < b.account? -1 : a.account == b.account? 0 : 1;}
    fuse = function(a,b) {return JSON.stringify({id: a.id, account: a.account, domain: a.domain, manager: b.manager })};
    emptyA = {id:0, account:"", domain: ""};
    emptyB = {id:0, account:"", manager: ""};
    joinStreams(streamA, streamB, outStream, comp, 'inner', fuse, emptyA, emptyB);

}

test1();
