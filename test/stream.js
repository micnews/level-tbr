

var tape = require('tape')

var tape             = require('tape')
var level            = require('level-test')()
var sublevel         = require('level-sublevel/bytewise')
var pull             = require('pull-stream')

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

  var sum = 0

  pull(
    tbr.query({name: 'foo', period: 'Hours', tail: true}),
    pull.drain(function (data) {
      console.log('data', data.key[2], data.value)
      sum += data.value
      if(+data.key[2] >= 4431600000) {
        console.log(sum)
        tbr.close()
        t.equal(sum, 302191)
        t.end()
      }
    })
  )
})
