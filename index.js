var TimeBucketReduce = require('time-bucket-reduce')
var tp = require('time-period')
var batchqueue = require('batchqueue')
var peek = require('level-peek')
var pull = require('pull-stream')
var pl = require('pull-level')
var cont = require('cont')
var tp = require('time-period')
var EventEmitter = require('events').EventEmitter

function last (db, query, period) {
  return function (cb) {
    peek.last(db, {
      gte: [query, period, 0],
      lt: [query, period, Number.MAX_VALUE]
    }, cb)
  }
}

const QUERY = 'foo'
const MAX = Number.MAX_VALUE

function revive(db, query, period) {
  var defer = pull.defer()
  query = QUERY
  peek.last(db, {
    gte: [query, period, 0],
    lt: [query, period, MAX]
  }, function (err, key, value) {
    //get the start of the next group.
    if(!key) return defer.resolve(pull.empty())

    var startNext = tp.ceil(key[2], period)
    var lowerPeriod = tp.periods[tp.periods.indexOf(period) - 1]
    defer.resolve(
      pl.read(db, {
        gte: [query, lowerPeriod, startNext],
        lt:  [query, lowerPeriod, MAX]
      })
    )
  })

  return defer
}


exports = module.exports = function (db, request) {

  var emitter = new EventEmitter(), latest = 0

  var query = 'foo'

  function init (db, TBR, query) {
    return cont.series(tp.periods.map(function (period, i) {
      return function (cb) {
        pull(
          revive(db, query, period),
          pull.drain(function (data) {
            var start = data.key[2]
            latest = Math.max(latest, start)
            TBR.rollup(i - 1, new Date(start), data.value)
          }, function () {
            cb()
          })
        )
      }
    }))
  }

  var queue = batchqueue(function (batch, cb) {
    db.batch(batch, function () {
      latest = batch.reduce(function (ts, b) {
        return Math.max(ts, b.ts)
      }, latest)
      cb()
    })
  }, function (err) {
    if(err) emitter.emit('error', err)
    else emitter.emit('drain')
  })

  var TBR = TimeBucketReduce({
      map: function (data) {
        return data.value
      },
      reduce: function (a, b) {
        return (a || 0) + b
      },
      output: function (value, start, type) {
        queue({
          key: [query, type, start], value: value,
          ts: start, type: 'put'
        })
      }
    })

  emitter.add = TBR

  emitter.dump = TBR.dump

  function initialize (query) {
    init(db, TBR, query) (function (err) {
      emitter.emit('ready')
    })
  }

  initialize()

  return emitter
}

//exports.latest = latest
exports.revive = revive

