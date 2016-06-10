require('proof')(7, require('cadence')(prove))

function prove (async, assert) {
    var cadence = require('cadence')
    var interrupt = require('interrupt')
    var Delta = require('delta')

    var Kibitzer = require('..')

    function copy (object) {
        return JSON.parse(JSON.stringify(object))
    }

    new Kibitzer(1, '1').terminate()

    var kibitzer = new Kibitzer(1, '1', { timeout: 1001 })
    assert(kibitzer.legislator.timeout, 1001, 'numeric timeout')
    kibitzer.terminate()

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
        syncLength: 24,
        ua: ua,
        __Date: { now: function () { return time } }
    }
    async(function () {
        kibitzers.push(new Kibitzer(1, createIdentifier(), extend({ properties: { location: createLocation() } }, options)))
    }, function () {
        kibitzers[0].bootstrap(async())
    }, function () {
        assert(kibitzers[0].legislator.properties[1].location, '127.0.0.1:8086', 'bootstraped')
        kibitzers.push(new Kibitzer(1, createIdentifier(), extend({ properties: { location: '127.0.0.1:8088' } }, options)))
        kibitzers[1].join({ location: '127.0.0.1:8086' }, async())
    }, function () {
        assert(kibitzers[1].getProperties().map(function (properties) {
            return properties.location
        }), [ '127.0.0.1:8086', '127.0.0.1:8088' ], 'joined')
        new Delta(async()).ee(kibitzers[1].log).on('entry')
    }, function (message) {
        assert(message.promise, '2/0', 'naturalized')
        var cookie = kibitzers[1].publish({ count: 1 })
        new Delta(async()).ee(kibitzers[1].log).on('entry')
    }, function (entry) {
        assert(entry.value, { count: 1 }, 'publish')
    }, function () {
        kibitzers[1]._enqueue({ entries: [{}] }, async())
    }, function (response) {
        assert(response, { posted: false, entries: [] }, 'failed enqueue')
        new Delta(async()).ee(kibitzers[1].log).on('terminated')
        kibitzers[1].terminate()
        kibitzers[1].terminate()
    }, function () {
        assert(true, 'terminated')
        kibitzers[0].terminate()
    }
}
