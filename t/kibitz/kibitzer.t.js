var cadence = require('cadence')
var UserAgent = require('vizsla')
var prolific = require('prolific')
var logger = prolific.createLogger('kibitz')

require('proof')(3, cadence(prove))

function prove (async, assert) {
    var Kibitzer = require('../..')

    var ua = new UserAgent

    var kibitzer = new Kibitzer('1', { timeout: 1001 })
    assert(kibitzer.timeout[0], 1001, 'numeric timeout')

    var port = 8086

    var identifier = 0
    function createIdentifier () {
        return String(++identifier)
    }

    function createURL () {
        return 'http://127.0.0.1:' + (port++)
    }

    var kibitzers = [], balancerIndex = 0, httpOkay = true
    var ua = {
        discover: cadence(function (async) {
            var kibitzer = kibitzers[balancerIndex]
            async(function () {
                kibitzer.discover(async())
            }, function (body) {
                return [ body, httpOkay ]
            })
        }),
        sync: cadence(function (async, url, post) {
            kibitzers.filter(function (kibitzer) {
                return kibitzer.url == url
            }).pop()._sync(post, async())
        }),
        enqueue: cadence(function (async, url, post) {
            kibitzers.filter(function (kibitzer) {
                return kibitzer.url == url
            }).pop()._enqueue(post, async())
        }),
        receive: cadence(function (async, url, post) {
            kibitzers.filter(function (kibitzer) {
                return kibitzer.url == url
            }).pop()._receive(post, async())
        })
    }
    var time = 0
    var options = {
        preferred: true,
        syncLength: 24,
        ua: ua,
        logger: function (level, message, context) {
            console.log('here')
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

    function extend (first, second) {
        for (var key in second) {
            first[key] = second[key]
        }
        return first
    }

    async(function () {
        kibitzers.push(new Kibitzer(createIdentifier(), extend({ url: createURL() }, options)))
        kibitzers[0].discover(async())
    }, function (body) {
        assert(body === null, 'no discovery')
        kibitzers[0].bootstrap()
        kibitzers[0].discover(async())
    }, function (body) {
        assert(body, {
            id: 'a10',
            islandId: 'a10',
            urls: [ 'http://127.0.0.1:8086' ]
        }, 'bootstrapped')
        kibitzers.push(new Kibitzer(createIdentifier(), extend({ url: createURL() }, options)))
        async([function () {
        kibitzers[1].join(async())
        }, function (error) {
            console.log(error.stack)
        }])
        async([function () {
            kibitzers[1]._checkSchedule2(async())
        }, function (error) {
            console.log(error.stack)
        }])
    }, function () {
        console.log('there')
    })

    return

    var kibitzer = new Kibitzer('1', {
        logger: function (level, message, context) {
            assert(message, 'test', 'catcher context')
            assert(level, 'error', 'exception message')
            assert(!context.error.unexeptional, 'exception')
        }
    })

    kibitzer.catcher('test')(new Error('caught'))

    var kibitzer = new Kibitzer('1', {
        logger: function (level, message, context) {
            assert(message, 'test', 'catcher context')
            assert(level, 'info', 'unexceptional exception message')
            assert(context.error.unexceptional, 'unexceptional exception')
        }
    })
    kibitzer.catcher('test')(kibitzer._unexceptional(new Error('caught')))

    kibitzer = new Kibitzer('1', {})
    kibitzer.logger('info', 'test', {}) // defaults

    try {
        kibitzer._checkPullIslandId('0')
    } catch (error) {
        assert(error.message, 'island change', 'check pull island id')
    }

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

    var balancer = new Balancer(new Binder('http://127.0.0.1:8080'))
    var options = {
        preferred: true,
        syncLength: 24,
        logger: function (level, message, context) {
            console.log('here')
            logger[level](message, context)
        },
        discovery: [ balancer.binder, { url: '/discover' } ]
    }

    var bouquet = new Bouquet
    var binder = new Binder('http://127.0.0.1:8086')
    var containers = [ new Container(binder, createIdentifier(), options) ]

    async(function () {
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
