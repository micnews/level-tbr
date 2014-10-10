
var tape     = require('tape')
var level    = require('level-test')()
var sublevel = require('level-sublevel/bytewise')
var pull     = require('pull-stream')

var LTBR = require('../')

tape('simple', function (t) {
  var _db = level('simple-tbr', {encoding: 'json'})
  var db = sublevel(_db)

  var start = 1413213123

  function request (j) {
    console.log('request', j)
    return pull(
      pull.infinite(function (i) {
        return j += 10000
      }),
      pull.take(function (i) {
        return i < 35 * 24*60*60*1000
      }),
      pull.map(function (i) {
        return {ts: start + i, value: 1}
      })
    )
  }

  var tbr = LTBR(db, request).addQuery({name: 'foo'})

  console.log(db.location)

  tbr.on('drain', function () {

    _db.close(function (err) {
      if(err) throw err

      var db2 = sublevel(level('simple-tbr', {encoding: 'json', clean: false}))

      var tbr2 = LTBR(db2, function () { return pull.empty() }).addQuery({name: 'foo'})

      tbr2.on('ready', function () {
        console.log(tbr.dump())
        console.log(tbr2.dump())
        tbr.close()
        tbr2.close()
        t.end()
      })

    })
  })

})
