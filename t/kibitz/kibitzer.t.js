require('proof')(8, require('cadence')(prove))

function prove (async, assert) {
    var cadence = require('cadence')
    var prolific = require('prolific')
    var logger = prolific.createLogger('kibitz')
    var interrupt = require('interrupt')
    var signal = require('signal')

    var Kibitzer = require('../..')

    function copy (object) {
        return JSON.parse(JSON.stringify(object))
    }

    var kibitzer = new Kibitzer('1', { timeout: 1001 })
    assert(kibitzer.timeout, 1001, 'numeric timeout')

    var port = 8086

    var identifier = 0
    function createIdentifier () {
        return String(++identifier)
    }

    function createLocation () {
        return '127.0.0.1:' + (port++)
    }

    signal.subscribe('.bigeasy.kibitz.log'.split('.'), function () {})

    // TODO Add `setImmediate` to assert asynchronicity.
    var kibitzers = [], balancerIndex = 0
    var ua = {
        discover: cadence(function (async) {
            return [ [ 'bogus' ].concat(kibitzers[balancerIndex].locations()) ]
        }),
        send: cadence(function (async, location, post) {
            async(function () {
                if (location == 'bogus') {
                    throw new Error
                }
                kibitzers.filter(function (kibitzer) {
                    return kibitzer.location == location
                }).pop().dispatch(copy(post), async())
            }, function (result) {
                return copy(result)
            })
        })
    }
    var time = 0
    var options = {
        preferred: true,
        syncLength: 24,
        ua: ua,
        logger: function (level, message, context) {
            logger[level](message, context)
        },
        Date: { now: function () { return time } },
        player: {
            brief: function () {
                return { next: null, entries: [] }
            },
            play: function (entry, callback) {
                callback()
            }
        }
    }

    async(function () {
        kibitzers.push(new Kibitzer(createIdentifier(), extend({ location: createLocation() }, options)))
    }, [function () {
        kibitzers[0]._sync(null, async())
    }, function (error) {
        interrupt.rescue('bigeasy.kibitz.unavailable', function () {
            assert(true, 'sync unavailable')
        })(error)
    }], [function () {
        kibitzers[0]._enqueue(null, async())
    }, function (error) {
        interrupt.rescue('bigeasy.kibitz.unavailable', function () {
            assert(true, 'enqueue unavailable')
        })(error)
    }], [function () {
        kibitzers[0]._receive(null, async())
    }, function (error) {
        interrupt.rescue('bigeasy.kibitz.unavailable', function () {
            assert(true, 'enqueue unavailable')
        })(error)
    }], function () {
        kibitzers[0].bootstrap()
        assert(kibitzers[0].locations(), [ '127.0.0.1:8086' ], 'locations')
        kibitzers.push(new Kibitzer(createIdentifier(), extend({ location: '127.0.0.1:8088' }, options)))
        kibitzers[1].join(async())
    }, function () {
        async([function () {
            var kibitzer = new Kibitzer('3', extend({
                location: createLocation()
            }, options, {
                ua: extend({}, ua, { send: cadence(function () { return null }) })
            }))
            kibitzer._pull('http://127.0.0.1:9090', async())
        }, function (error) {
            interrupt.rescue('bigeasy.kibitz.pull', function () {
                assert(true, 'pull failed')
            })(error)
        }])
    }, function () {
        async([function () {
            var kibitzer = new Kibitzer('3', extend({
                location: createLocation()
            }, options, {
                ua: extend({}, ua, { discover: cadence(function () { return [[]] }) })
            }))
            kibitzer.join(async())
        }, function (error) {
            interrupt.rescue('bigeasy.kibitz.discover', function () {
                assert(true, 'discover failed')
            })(error)
        }])
    }, function () {
        kibitzers.push(new Kibitzer(createIdentifier(), extend({ location: createLocation() }, options)))
        kibitzers[2].join(async())
    }, function () {
        kibitzers.push(new Kibitzer(createIdentifier(), extend({ location: createLocation() }, options)))
        kibitzers[3].join(async())
    }, function () {
        kibitzers.push(new Kibitzer(createIdentifier(), extend({ location: createLocation() }, options)))
        kibitzers[4].join(async())
    }, function () {
        assert(kibitzers[0].locations().length, 5, 'full consensus')
    }, function () {
        kibitzers[4]._enqueue({
            entries: [{
                cookie: '20/1',
                value: { type: 'naturalize', id: '20', location: '127.0.0.1:8088' },
                internal: true
            }]
        }, async())
    })

    function extend (to) {
        var vargs = [].slice.call(arguments, 1)
        for (var i = 0, I = vargs.length; i < I; i++) {
            for (var key in vargs[i]) {
                to[key] = vargs[i][key]
            }
        }
        return to
    }
}
