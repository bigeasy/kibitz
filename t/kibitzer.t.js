require('proof/redux')(7, require('cadence')(prove))

function prove (async, assert) {
    var cadence = require('cadence')
    var delta = require('delta')

    var Kibitzer = require('..')

    function copy (object) {
        return JSON.parse(JSON.stringify(object))
    }

    new Kibitzer({ kibitzerId: '1', ua: {} }).shutdown()

    var kibitzer = new Kibitzer({ kibitzerId: '1', timeout: 1001, ua: {} })
    assert(kibitzer.legislator.timeout, 1001, 'numeric timeout')
    kibitzer.shutdown()

    var port = 8086, identifier = 0
    function createIdentifier () { return String(++identifier) }
    function createLocation () { return '127.0.0.1:' + (port++) }

    var kibitzers = [], balancerIndex = 0
    var ua = {
        send: cadence(function (async, properties, post) {
            async(function () {
                if (properties == 'bogus') {
                    throw new Error
                }
                kibitzers.filter(function (kibitzer) {
                    return kibitzer.properties.location == properties.location
                }).pop().dispatch(copy(post), async())
            }, function (result) {
                return [ copy(result) ]
            })
        })
    }

    var time = 0, options = {
        notify: function () {},
        syncLength: 24,
        ua: ua,
        __Date: { now: function () { return time } }
    }
    async(function () {
        kibitzers.push(new Kibitzer(extend({
            kibitzerId: createIdentifier(),
            properties: { location: createLocation() }
        }, options)))
    }, function () {
        kibitzers[0].bootstrap(0)
    }, function () {
        assert(kibitzers[0].legislator.properties[1].location, '127.0.0.1:8086', 'bootstraped')
        kibitzers.push(new Kibitzer(extend({
            kibitzerId: createIdentifier(),
            properties: { location: '127.0.0.1:8088' }
        }, options)))
        kibitzers[1].join({ location: '127.0.0.1:8086', islandId: 0 }, async())
        delta(async()).ee(kibitzers[1]).on('enqueued')
    }, function () {
        assert(kibitzers[1].shift().type, 'join', 'joined')
        assert(kibitzers[1].shift().entry.promise, '2/0', 'immigrated')
        var cookie = kibitzers[1].publish({ count: 1 })
        var loop = async(function () {
            async(function () {
                delta(async()).ee(kibitzers[1]).on('enqueued')
            }, function () {
                var message = kibitzers[1].shift()
                if (message != null) {
                    return [ loop.break, message ]
                }
            })
        })()
    }, function (message) {
        assert(message.entry.value, { count: 1 }, 'publish')
    }, function () {
        kibitzers[1]._enqueue({ entries: [{}] }, async())
    }, function (response) {
        assert(response, [], 'failed enqueue')
        kibitzers[1].shutdown()
        kibitzers[1].shutdown()
    }, function () {
        assert(true, 'terminated')
        kibitzers[0].shutdown()
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
