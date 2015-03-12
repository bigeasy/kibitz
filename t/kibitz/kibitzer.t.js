var cadence = require('cadence/redux')
var UserAgent = require('inlet/http/ua')

require('proof')(15, cadence(prove))

function prove (async, assert) {
    var Kibitzer = require('../..'),
        Balancer = require('../balancer'),
        Container = require('../container'),
        Binder = require('inlet/net/binder'),
        Bouquet = require('inlet/net/bouquet')

    var ua = new UserAgent

    var kibitzer = new Kibitzer('1', {
        logger: function (level, message, context) {
            assert(message, 'test', 'catcher context')
            if (!context.error) throw new Error
            assert(context.error.message, 'catcher caught')
        },
        preferred: true
    })

    kibitzer.catcher('test')(new Error('caught'))
    kibitzer = new Kibitzer('1', {})
    kibitzer.logger('info', 'test', {}) // defaults

    try {
        kibitzer._checkPullIslandId('0')
    } catch (error) {
        assert(error.message, 'island change', 'check pull island id')
    }
/*    kibitzer._discover({
        raise: function (statusCode) {
            assert(statusCode, 517, 'discover with no island')
        }
    }, function () {}) */

    kibitzer._sync({
        body: { islandId: 'a' },
        raise: function (statusCode) {
            assert(statusCode, 517, 'sync with wrong island')
        }
    }, function () {})

    kibitzer._enqueue({
        body: { islandId: 'a' },
        raise: function (statusCode) {
            assert(statusCode, 517, 'enqueue with wrong island')
        }
    }, function () {})

    kibitzer._receive({
        body: { islandId: 'a' },
        raise: function (statusCode) {
            assert(statusCode, 517, 'receive with wrong island')
        }
    }, function () {})

    assert(kibitzer._response({ okay: false }, null, null, null, 1), 1, 'not okay')
    assert(kibitzer._response({ okay: true }, { posted: false }, 'posted', null, 1), 1, 'value missing')

    var identifier = 0
    function createIdentifier () {
        return String(++identifier)
    }

    var balancer = new Balancer(new Binder('http://127.0.0.1:8080'))
    var options = {
        preferred: true,
        syncLength: 24,
        discovery: [ balancer.binder, { url: '/discover' } ]
    }

    var bouquet = new Bouquet
    var binder = new Binder('http://127.0.0.1:8086')
    var container = new Container(binder, 'f', {
        discovery: [ balancer.binder, { url: '/discover' } ],
        logger: function (level, message, context) {
            if (message == 'join') {
                throw new Error
            } else if (level == 'error') {
                assert(!context.error.unexceptional, 'exceptional error')
            }
        }
    })
    var containers = [ new Container(binder, createIdentifier(), options) ]

    async(function () {
        container.kibitzer.whenJoin(async())
    }, function () {
        container.kibitzer.stop(async())
    }, function () {
        bouquet.start(balancer, async())
    }, function () {
        bouquet.start(containers[0], async())
    }, function () {
        containers[0].kibitzer.join(async())
        containers[0].kibitzer.join(async())
    }, function () {
        containers[0].kibitzer.join(async())
    }, function () {
        balancer.servers.push(containers[0].binder)
        var binder = new Binder('http://127.0.0.1:8087')
        containers.push(new Container(binder, createIdentifier(), options))
        bouquet.start(containers[1], async())
    }, function () {
        containers[1].kibitzer.join(async())
    }, function () {
        assert(containers[1].kibitzer.legislator.government.constituents.length, 1, 'registered first participant')
    }, function () {
        options.preferred = false
        var binder = new Binder('http://127.0.0.1:8088')
        containers.push(new Container(binder, createIdentifier(), options))
        bouquet.start(containers[2], async())
    }, function () {
        containers[2].kibitzer.join(async())
    }, function () {
        containers[1].kibitzer.wait('2/0', async())
    }, function () {
        assert(containers[1].kibitzer.legislator.government.majority.length, 2, 'registered second participant')
        ua.fetch({
            url: containers[1].kibitzer.url
        }, {
            url: '/discover'
        }, async())
    }, function (body, response) {
        containers[0].kibitzer.publish({ type: 'add', key: 1, value: 'a' }, async())
    }, function () {
        containers[1].kibitzer.wait('2/1', async())
        containers[1].kibitzer.wait('2/1', async())
    }, function () {
        // test waiting for something that has already arrived.
        containers[1].kibitzer.wait('2/1', async())
    }, function () {
        containers[1].lookup.each(function (entry) {
            console.log(entry)
        })
        var i = 0, loop = async(function () {
            if (i++ === 48) return [ loop ]
            async(function () {
                containers[2].kibitzer.publish({ type: 'add', key: i, value: 'a' }, async())
            })
        })()
    }, function () {
        var binder = new Binder('http://127.0.0.1:8089')
        containers.push(new Container(binder, createIdentifier(), options))
        bouquet.start(containers[3], async())
    }, function () {
        containers[3].kibitzer.join(async())
    }, function () {
        assert(containers[3].kibitzer.legislator.government.majority.length, 2, 'registered third participant')
    }, function () {
        var binder = new Binder('http://127.0.0.1:8090')
        containers.push(new Container(binder, createIdentifier(), options))
        bouquet.start(containers[4], async())
    }, function () {
        containers[4].kibitzer.join(async())
    }, function () {
        setTimeout(async(), 3000)
    }, function () {
        assert(containers[4].kibitzer.legislator.government.majority.length, 3, 'registered fourth participant')
        containers[4].kibitzer._enqueue({
            body: {
                islandId: 'a10',
                entries: [{ cookie: 'x', value: 1, internal: false }]
            }
        }, async())
    }, function (response) {
        assert(!response.posted, 'enqueue not leader')
    }, [function () {
        containers[4].kibitzer.pull('http://127.0.0.1:8091', async())
    }, function (error) {
        assert(error.message, 'unable to sync', 'cannot sync')
    }], function () {
        setTimeout(async(), 350)
    }, function () {
        async.forEach(function (container) {
            container.kibitzer.stop(async())
        })(containers)
    }, function () {
        async.forEach(function (container) {
            container.kibitzer.stop(async())
        })(containers)
    }, function () {
        bouquet.stop(async())
    })
}
