var cadence = require('cadence/redux')
var UserAgent = require('inlet/http/ua')

require('proof')(6, cadence(prove))

function prove (async, assert) {
    var Kibitzer = require('../..'),
        Balancer = require('../balancer'),
        Container = require('../container'),
        Binder = require('inlet/net/binder'),
        Bouquet = require('inlet/net/bouquet')

    var ua = new UserAgent

    var identifier = 0
    function createIdentifier () {
        return String(++identifier)
    }

    var bouquet = new Bouquet
    var balanced = new Bouquet

    var balancer = new Balancer(new Binder('http://127.0.0.1:8080'))
    var options = {
        ping: [ 150, 350 ],
        preferred: false,
        discovery: [ balancer.binder, { url: '/discover' } ]
    }
    var port = 8086
    var containers = []

    async(function () {
        bouquet.start(balancer, async())
    }, function () {
        async(function () {
            console.log('starting unbalanced')
            var container, binder, joined = 0
            var loop = async(function () {
                if (containers.length == 3) return [ loop ]
                binder = new Binder('http://127.0.0.1:' + port++)
                container = new Container(binder, createIdentifier(), options)
                containers.push(container)
                balanced.start(container, async())
            }, function () {
                var id = container.kibitzer.legislator.id
                container.kibitzer.join(function (error) {
                        console.log("JOINED", id)
                    if (error) throw error
                    if (++joined === 3) {
                        assert(true, 'unbalanced joined')
                    }
                })
            })()
        }, function () {
            console.log('unbalanced started')
        })
        async(function () {
            setTimeout(async(), 1000)
        }, function () {
            console.log('starting balanced')
            var binder, container, joined = 0
            options = { preferred: true, discovery: options.discovery }
            var loop = async(function () {
                if (containers.length == 5) return [ loop ]
                binder = new Binder('http://127.0.0.1:' + port++)
                container = new Container(binder, createIdentifier(), options)
                containers.push(container)
                balanced.start(container, async())
            }, function () {
                container.kibitzer.join(function (error) {
                    if (error) throw error
                    if (++joined === 2) {
                        assert(true, 'balanced joined')
                    }
                })
            })()
        }, function () {
            console.log('balanced started')
        })
    }, function () {
        console.log('island started')
        setTimeout(async(), 1000)
    }, function () {
        assert(containers[3].kibitzer.islandId, 'a40', 'bootstrapped')
        assert(containers[4].kibitzer.islandId, 'a50', 'split brain')
        balancer.servers.push(containers[4].binder)
        setTimeout(async(), 1000)
    }, function () {
        balancer.servers.push(containers[3].binder)
        var loop = async(function () {
            setTimeout(async(), 1000)
        }, function () {
            if (containers[4].kibitzer.islandId == 'a40') {
                return [ loop ]
            }
        })()
    }, function () {
        console.log("WHAT!!!", containers[2].kibitzer.happenstance.what[73], Date.now())
        assert(true, 'killed preferred')
        var loop = async(function () {
            if (containers.slice(0, 2).every(function (container) {
                return container.kibitzer.islandId == 'a40'
            })) {
                return [ loop ]
            }
        }, function () {
            setTimeout(async(), 1000)
        })()
    }, function () {
        setTimeout(async(), 3000)
    }, function () {
        assert(true, 'killed not preferred')
        async.forEach(function (container) {
            container.kibitzer.stop(async())
        })(containers)
    }, function () {
        balanced.stop(async())
    }, function () {
        bouquet.stop(async())
    })
}
