var TimeBucketReduce = require('time-bucket-reduce')
var batchqueue       = require('batchqueue')
var peek             = require('level-peek')
var pull             = require('pull-stream')
var pl               = require('pull-level')
var toStream         = require('pull-stream-to-stream')
var cont             = require('cont')
var tp               = require('time-period')
var ltgt             = require('ltgt')
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

function toTimestamp(d) {
  return d
  if(null == d) return d
  var e = new Date(d)
  if('Invalid Date' == e) {
    e = new Date(+d)
    if('Invalid Date' == e)
      throw new Error('Invalid Date:' + d)
  }
  return +e
}

exports = module.exports = function (db, request) {

  if('function' !== typeof request)
    throw new Error('must provide request(latest) function')

  var emitter = new EventEmitter(), latest = 0
  var queries = emitter.queries = {}

  function revive(db, query, period) {
    var defer = pull.defer()
    peek.last(db, {
      gte: [query, period, 0],
      lt: [query, period, undefined]
    }, function (err, key, value) {
      //get the start of the next group.
      if(!key) return defer.resolve(pull.empty())
      var startNext = tp.ceil(key[2], period)
      var lowerPeriod = tp.periods[tp.periods.indexOf(period) - 1]

      latest = Math.max(latest, key[2])
      defer.resolve(
        pull(
          pl.read(db, {
            gte: [query, lowerPeriod, startNext],
            lt:  [query, lowerPeriod, MAX]
          }),
          pull.through(console.log)
        )

      )
    })

    return defer
  }


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

  var started = false
  function start () {
    if(emitter.closed) return
    emitter.emit('reconnect', emitter.closed)
    pull(
      request(latest),
      pull.drain(emitter.add, function () {
        emitter.emit('ended')

        if(emitter.closed) return
        console.log('reconnect')
        setTimeout(start, 1000 + Math.random() * 3000)
      })
    )
  }

  emitter.on('ready', function () {
    if(started) return
    started = true
    start()
  })

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
            key: [name, type, start], value: value,
            ts: start, type: 'put'
          })

          emitter.emit(type, name, value, start)
          emitter.emit(name, type, value, start)
          emitter.emit(name+':'+type, name, value, start, type)
        }
      })
    return emitter
  }

  emitter.query = function (opts) {
    var name   = opts && opts.name
    var period = opts && opts.period

    if(!name)
      throw new Error('must provide period')

    if(!~tp.periods.indexOf(period))
      throw new Error('period must be one of:' + JSON.stringify(tp.periods))

    function map (key) {
      return [name, period, toTimestamp(key)]
    }

    opts = ltgt.toLtgt(opts, opts, map, 0, MAX)

    return pl.read(db, opts)
  }

  //expose over multilevel.
  emitter.install = function (db) {
    db.methods.createQueryStream = {type: 'readable'}
    db.createQueryStream = function (opts) {
      return toStream.source(emitter.query(opts))
    }
  }

  emitter.close = function () {
    emitter.closed = true
  }

  return emitter
}

