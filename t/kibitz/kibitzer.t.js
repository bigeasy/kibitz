require('proof')(5, require('cadence')(prove))

function prove (async, assert) {
    var cadence = require('cadence')
    var UserAgent = require('vizsla')
    var prolific = require('prolific')
    var logger = prolific.createLogger('kibitz')
    var interrupt = require('interrupt')
    var signal = require('signal')

    var Kibitzer = require('../..')

    var ua = new UserAgent

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
    var kibitzers = [], balancerIndex = 0, httpOkay = true
    var ua = {
        discover: cadence(function (async) {
            return [ kibitzers[balancerIndex].locations(), httpOkay ]
        }),
        sync: cadence(function (async, location, post) {
            async(function () {
                kibitzers.filter(function (kibitzer) {
                    return kibitzer.location == location
                }).pop()._sync(copy(post), async())
            }, function (result) {
                return copy(result)
            })

        }),
        enqueue: cadence(function (async, location, post) {
            async(function () {
                kibitzers.filter(function (kibitzer) {
                    return kibitzer.location == location
                }).pop()._enqueue(copy(post), async())
            }, function (result) {
                return copy(result)
            })
        }),
        receive: cadence(function (async, location, post) {
            async(function () {
                kibitzers.filter(function (kibitzer) {
                    return kibitzer.location == location
                }).pop()._receive(post, async())
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

    function extend (first, second) {
        for (var key in second) {
            first[key] = second[key]
        }
        return first
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
        kibitzers.push(new Kibitzer(createIdentifier(), extend({ location: createLocation() }, options)))
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
                islandId: '10',
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
