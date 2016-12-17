var iterate = require('stream-iterate')
var from = require('from2')
const compareUtils = require('./comparer.js')

var union = function (streamA, streamB, compare) {
  var readA = iterate(streamA)
  var readB = iterate(streamB)

  if (!compare) compare = compareUtils.defaultCompare

  var stream = from.obj(function loop (size, cb) {
    readA(function (err, dataA, nextA) {
      if (err) return cb(err)
      readB(function (err, dataB, nextB) {
        if (err) return cb(err)

        if (!dataA && !dataB) return cb(null, null)

        if (!dataA) {
          nextB()
          return cb(null, dataB)
        }

        if (!dataB) {
          nextA()
          return cb(null, dataA)
        }

        if (compare(dataA, dataB) <= 0) {
          nextA()
          return cb(null, dataA)
        }

        nextB()
        cb(null, dataB)
      })
    })
  })

  stream.on('close', function () {
    if (streamA.destroy) streamA.destroy()
    if (streamB.destroy) streamB.destroy()
  })

  return stream
}

module.exports = union
