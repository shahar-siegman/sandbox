'use strict'
const fs = require('fs')
const path = require('path')
const es = require('event-stream')
const defaultFile = './kajero-save-temp.txt'
const notebookWriter = require('./notebookWriter')
// make sure it's a valid path
var watchDirName = process.argv.length > 2 && process.argv[2]
if (!watchDirName) {
    console.log('no watch dir specified')
    process.exit(-1)
}
watchDirName = fs.realpathSync(watchDirName)
if (!fs.statSync(watchDirName).isDirectory()) {
    console.log('Cannot resolve ' + watchDirName + '; not a valid directory.')
    process.exit(-1)
}

// now set up a watch
console.log('Watching ' + watchDirName)
fs.watch(watchDirName, {}, function (event, filename) {
    console.log('detected ' + event + ' to ' + filename)
    if (filename.match(/^kajero_download( [\(\)0-9]+)?\.txt$/) && event == 'rename') {
        console.log('!!!detected rename of ' + filename)
        // now extact the URI
        setTimeout(function () {
            var lines = [], url;
            fs.createReadStream(path.join(watchDirName, filename), 'utf8')
                .pipe(es.split())
                .pipe(es.through(function (line) {
                    lines.push(line)
                    if (!url) {
                        url = line.match(/^\s*url: \"(.+)\"/)
                        url = url && (url.length >= 2) && url[1]
                    }
                }, function () {
                    var filename = normalizeUri(url)
                    if (!filename) {
                        console.warn('File location not detected. saving a temporary file instead')
                        filename = defaultFile;
                    }
                    try {
                        fs.writeFileSync(filename, notebookWriter(lines), { encoding: 'utf8' })
                    } catch (error) {
                        console.error("file save failed: " + error.toString())
                    }
                }))

        }, 100);
    }
})

function isLocalFileUri(uri) {
    return uri && uri.length > 8 && uri.substring(0, 8) == 'file:///'
}
function normalizeUri(uri) {
    if (isLocalFileUri(uri))
        return path.normalize(uri.substring(8))
}