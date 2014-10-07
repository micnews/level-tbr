var TimeBucketReduce = require('time-bucket-reduce')
var batchqueue       = require('batchqueue')
var peek             = require('level-peek')
var pull             = require('pull-stream')
var pl               = require('pull-level')
var cont             = require('cont')
var tp               = require('time-period')
var EventEmitter     = require('events').EventEmitter

function last (db, query, period) {
  return function (cb) {
    peek.last(db, {
      gte: [query, period, 0],
      lt: [query, period, Number.MAX_VALUE]
    }, cb)
  }
}

const MAX = Number.MAX_VALUE

function revive(db, query, period) {
  var defer = pull.defer()
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
  var queries = emitter.queries = {}
  var query = 'foo'

  function init (db, TBR, query) {
    return cont.series(tp.periods.map(function (period, i) {
      return function (cb) {
        pull(
          revive(db, query, period),
          pull.drain(function (data) {
            var start = data.key[2]
            latest = Math.max(latest, start)
            queries[query].rollup(i - 1, new Date(start), data.value)
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


  emitter.add = function (data) {
    for(var k in queries)
      queries[k](data)
  }

  emitter.dump = function () {
    var o = {}
    for(var k in queries)
      o[k] = queries[k].dump()
    return o
  }

  function initialize () {
    var o = {}
    for(var q in queries)
      o[q] = init(db, queries[q], q)

    cont.para(o) (function (err) {
      emitter.emit('ready')
    })
  }

  setImmediate(initialize)

  emitter.addQuery = function (opts) {
    if(!opts) throw new Error('must have opts')
    if(!opts.name) throw new Error('must have query name')
    var name = opts.name
    queries[name] = TimeBucketReduce({
        map: opts.map || function (data) {
          return data.value
        },
        reduce: opts.reduce || function (a, b) {
          return (a || 0) + b
        },
        output: function (value, start, type) {
          queue({
            key: [query, type, start], value: value,
            ts: start, type: 'put'
          })
        }
      })
    return emitter
  }

  return emitter
}

//exports.latest = latest
exports.revive = revive

