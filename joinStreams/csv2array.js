const through = require('through')
const lbl = require('n-readlines')
const fs = require('fs')
const Readable = require('stream').Readable;
const lazypipe = require('lazypipe')

lineReadStream = function (pathOrFD, options) {
    var f = new lbl(pathOrFD, options);
    var reader = new Readable({ encoding: 'utf8' });
    reader._read = function () {
        var line = f.next();
        if (line)
            this.push(line);
        else
            this.push(null);

    }
    return reader;
}

transformCSVline = function (sampleFields) {
    if (sampleFields && !Array.isArray(sampleFields))
        sampleFields = Object.keys(sampleFields);
    var ret =
        through(function (line) {
            var a = CSVtoArray(line);
            if (null == a)
                throw 'parse error!';
            if (sampleFields) {
                var convertLength = Math.min(sampleFields.length, a.length);
                var objectToStream = {};
                for (var i = 0; i < convertLength; i++)
                    objectToStream[sampleFields[i]] = a[i];
            }
            else
                objectToStream = a;
            this.queue(objectToStream);
        },
            function () {
                this.queue(null);
            }
        )
    return ret;
}

CSVtoArray = function (text) {
    var re_valid = /^\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*(?:,\s*(?:'[^'\\]*(?:\\[\S\s][^'\\]*)*'|"[^"\\]*(?:\\[\S\s][^"\\]*)*"|[^,'"\s\\]*(?:\s+[^,'"\s\\]+)*)\s*)*$/;
    var re_value = /(?!\s*$)\s*(?:'([^'\\]*(?:\\[\S\s][^'\\]*)*)'|"([^"\\]*(?:\\[\S\s][^"\\]*)*)"|([^,'"\s\\]*(?:\s+[^,'"\s\\]+)*))\s*(?:,|$)/g;
    // Return NULL if input string is not well formed CSV string.
    if (!re_valid.test(text)) return null;
    var a = [];                     // Initialize array to receive values.
    text.replace(re_value, // "Walk" the string using replace with callback.
        function (m0, m1, m2, m3) {
            // Remove backslash from \' in single quoted values.
            if (m1 !== undefined) a.push(m1.replace(/\\'/g, "'"));
            // Remove backslash from \" in double quoted values.
            else if (m2 !== undefined) a.push(m2.replace(/\\"/g, '"'));
            else if (m3 !== undefined) a.push(m3);
            return ''; // Return empty string.
        });
    // Handle special case of empty last value.
    if (/,\s*$/.test(text)) a.push('');
    return a;
}


module.exports = {
    CSVfileReader: function (pathOrFD, sampleFields, options) {
        //return lazypipe().pipe(lineReadStream,pathOrFD, options).pipe(transformCSVline ,sampleFields)
        var f = new lbl(pathOrFD, options);
        var ret = new Readable({ objectMode: true });
        if (sampleFields && !Array.isArray(sampleFields))
            sampleFields = Object.keys(sampleFields);

        ret._read = function () {
            var line = f.next()
            if (line) {
                var stringLine=line.toString().trim();
                var a = CSVtoArray(stringLine);
                if (null == a)
                    throw 'parse error!';
                if (sampleFields) {
                    var convertLength = Math.min(sampleFields.length, a.length);
                    var objectToStream = {};
                    for (var i = 0; i < convertLength; i++)
                        objectToStream[sampleFields[i]] = a[i];
                }
                else
                    objectToStream = a;
                this.push(objectToStream);
            }
            else
                this.push(null);
        }
        return ret;
    }
}

