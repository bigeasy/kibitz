require('proof')(7, require('cadence')(prove))

function prove (async, assert) {
    var cadence = require('cadence')
    var interrupt = require('interrupt')
    var signal = require('signal')

    var Kibitzer = require('../..')

    function copy (object) {
        return JSON.parse(JSON.stringify(object))
    }

    new Kibitzer(1, '1')

    var kibitzer = new Kibitzer(1, '1', { timeout: 1001 })
    assert(kibitzer.legislator.timeout, 1001, 'numeric timeout')

    var port = 8086, identifier = 0
    function createIdentifier () { return String(++identifier) }
    function createLocation () { return '127.0.0.1:' + (port++) }

    signal.subscribe('.bigeasy.kibitz.log'.split('.'), function () {
        // console.log([].slice.call(arguments, 2))
    })

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
                return [ copy(result) ]
            })
        })
    }

    var time = 0, options = {
        syncLength: 24,
        ua: ua,
        __Date: { now: function () { return time } }
    }
    async(function () {
        kibitzers.push(new Kibitzer(1, createIdentifier(), extend({ location: createLocation() }, options)))
    }, function () {
        kibitzers[0].bootstrap(async())
    }, function () {
        assert(kibitzers[0].locations(), [ '127.0.0.1:8086' ], 'bootstraped')
        kibitzers.push(new Kibitzer(1, createIdentifier(), extend({ location: '127.0.0.1:8088' }, options)))
        kibitzers[1].join(async())
    }, function () {
        assert(kibitzers[1].locations(), [ '127.0.0.1:8086', '127.0.0.1:8088' ], 'joined')
        kibitzers[1].advance(async())
    }, function (message) {
        assert(message.promise, '2/0', 'naturalized')
        var cookie = kibitzers[1].publish({ count: 1 })
        kibitzers[1].advance(async())
    }, function (entry) {
        assert(entry.value.value, { count: 1 }, 'publish')
    }, function () {
        async([function () {
            var kibitzer = new Kibitzer(1, '2', extend({
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
        kibitzers[1]._enqueue({ entries: [{}] }, async())
    }, function (response) {
        assert(response, { posted: false, entries: [] }, 'failed enqueue')
        return [ async.break ]
        async([function () {
            var kibitzer = new Kibitzer(1, '3', extend({
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
        kibitzers.push(new Kibitzer(1, createIdentifier(), extend({ location: createLocation() }, options)))
        kibitzers[2].join(async())
    }, function () {
        kibitzers.push(new Kibitzer(1, createIdentifier(), extend({ location: createLocation() }, options)))
        kibitzers[3].join(async())
    }, function () {
        kibitzers.push(new Kibitzer(1, createIdentifier(), extend({ location: createLocation() }, options)))
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
