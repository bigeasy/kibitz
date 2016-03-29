var assert = require('assert')
var url = require('url')

var signal = require('signal')

var cadence = require('cadence')

var Reactor = require('reactor')

var sequester = require('sequester')
var Id = require('paxos/id')
var Legislator = require('paxos/legislator')
var Client = require('paxos/client')

var Monotonic = require('monotonic')

var Scheduler = require('happenstance')

var interrupt = require('interrupt').createInterrupter('bigeasy.kibitz')

function Kibitzer (id, options) {
    assert(id != null, 'id is required')

    options.ping || (options.ping = 250)
    options.timeout || (options.timeout = 1000)
    this.poll = options.poll || 5000
    this.ping = options.ping
    this.timeout = options.timeout

    this.syncLength = options.syncLength || 250

    this._reactor = new Reactor({ object: this, method: '_tick' })

    this.happenstance = new Scheduler
    this._Date = options.Date || Date

    this.joining = []

    this.location = options.location
    this._ua = options.ua
    this._join = sequester.createLock()

    this.baseId = id
    this.suffix = '0'

    this.legislator = this._createLegislator()
    this.client = new Client(this.legislator.id)

    this.cookies = {}

    this.discovery = options.discovery

    this.available = false
}

Kibitzer.prototype._logger = function (level, message, context) {
    var subscribers = signal.subscribers([ '', 'bigeasy', 'kibitz', 'log' ])
    for (var i = 0, I = subscribers.length; i < I; i++) {
        subscribers[i](this, level, message, context)
    }
}

Kibitzer.prototype._createLegislator = function () {
    var suffix = this.suffix
    var words = Monotonic.parse(this.suffix)
    this.suffix = Monotonic.toString(Monotonic.increment(words))
    return new Legislator(this.baseId + suffix, {
        ping: this.ping,
        timeout: this.timeout
    })
}

Kibitzer.prototype._tick = cadence(function (async) {
    var dirty = false
    async(function () {
        var outgoing = this.client.outbox()
        if (outgoing.length) {
            var post
            async(function () {
                var location = this.legislator.locations[this.legislator.government.majority[0]]
                this._ua.send(location, post = {
                    type: 'enqueue',
                    entries: outgoing
                }, async())
            }, function (body) {
                assert.ok(body.entries)
                this._logger('info', 'enqueued', {
                    kibitzerId: this.legislator.id,
                    sent: post,
                    received: body
                })
                this.client.published(body.entries)
            })
            dirty = true
        }
    }, function () {
        async.forEach(function (route) {
            var forwards = this.legislator.forwards(this._Date.now(), route, 0), serialized
            async(function () {
                serialized = {
                    type: 'receive',
                    route: route,
                    index: 1,
                    messages: forwards
                }
                var location = this.legislator.locations[route.path[1]]
                this._ua.send(location, serialized, async())
            }, function (body) {
                assert.ok(body.returns)
                var returns = body.returns
                this._logger('info', 'published', {
                    kibitzerId: this.legislator.id,
                    sent: serialized,
                    received: body
                })
                this.legislator.inbox(this._Date.now(), route, returns)
                this.legislator.sent(this._Date.now(), route, forwards, returns)
            })
            dirty = true
        })(this.legislator.outbox())
    }, function () {
        var entries = this.legislator.since(this.client.uniform)
        if (entries.length) {
            this._logger('info', 'consuming', {
                kibitzerId: this.legislator.id,
                entries: entries
            })
            var promise = this.client.receive(entries)
            async.forEach(function (entry) {
                this._logger('info', 'consume', {
                    kibitzerId: this.legislator.id,
                    entry: entry
                })
                var callback = this.cookies[entry.cookie]
                if (callback) {
                    callback(null, entry)
                    delete this.cookies[entry.cookie]
                }
            })(this.client.since(promise))
            dirty = true
        }
    }, function () {
        if (dirty) {
            this._reactor.check()
        }
    })
})

Kibitzer.prototype.locations = function () {
    var locations = []
    for (var key in this.legislator.locations) {
        locations.push(this.legislator.locations[key])
    }
    return locations
}

Kibitzer.prototype._pull = cadence(function (async, location) {
    assert(location, 'url is missing')
    var dataset = 'log', post, next = null
    var sync = async(function () {
        this._ua.send(location, post = {
            type: 'sync',
            kibitzerId: this.legislator.id,
            dataset: dataset,
            next: next
        }, async())
    }, function (body) {
        this._logger('info', 'pulled', {
            kibitzerId: this.legislator.id,
            location: location,
            sent: JSON.stringify(post),
            received: JSON.stringify(post)
        })
        if (!body) { // TODO When does this happen? Why not just raise?
            throw interrupt(new Error('pull'))
        } else {
            this.legislator.inject(body.entries)
            if (body.next == null) {
                return [ sync.break ]
            }
            next = body.next
        }
    })()
})

Kibitzer.prototype._sync = cadence(function (async, post) {
    if (!this.available) {
        this._logger('info', 'sync', {
            kibitzerId: this.legislator.id,
            available: this.available,
            received: post
        })
        throw interrupt(new Error('unavailable'))
    }
    var response = this.legislator.extract('reverse', 24, post.next)
    this._logger('info', 'sync', {
        kibitzerId: this.legislator.id,
        available: this.available,
        post: post,
        response: response
    })
    return response
})

Kibitzer.prototype.bootstrap = function (async) {
    this.bootstrapped = true
    this._reactor.turnstile.turnstiles = 1
    this.legislator.bootstrap(this._Date.now(), this.location)
    this._logger('info', 'bootstrap', {
        kibitzerId: this.legislator.id
    })
    this.client.prime(this.legislator.prime('1/0'))
    this.available = true
}

Kibitzer.prototype.join = cadence(function (async) {
    async(function () {
        this._ua.discover(async())
    }, function (locations) {
        this._logger('info', 'locations', {
            kibitzerId: this.legislator.id,
            received: JSON.stringify(locations)
        })
        var location, loop = async(function () {
            location = locations.shift()
            if (!location) {
                throw interrupt(new Error('discover'))
            }
            async([function () {
                this._ua.send(location, { type: 'health' }, async())
            }, function (error) {
                this._logger('info', 'health', { location: location, healthy: false })
                return [ loop.continue ]
            }], function () {
                this._logger('info', 'health', { location: location, healthy: true })
                return [ loop.break, location ]
            })
        })()
    }, function (location) {
        this._logger('info', 'join', {
            kibitzerId: this.legislator.id,
            location: location
        })
        this._pull(location, async())
    }, function () {
        this.legislator.immigrate(this.legislator.id)
        this.legislator.initialize(this._Date.now())
        var since = this.legislator.min()
        this.client.prime(this.legislator.since(since, 1))
        for (;;) {
            var entries = this.legislator.since(since, 24)
            if (entries.length == 0) {
                break
            }
            since = this.client.receive(entries)
        }
        this.bootstrapped = false
        var since = this.legislator._greatestOf(this.legislator.id).uniform
        this.client.prime(this.legislator.prime(since))
        assert(this.client.length, 'no entries in client')
        this._reactor.turnstile.turnstiles = 1
        this._reactor.check()
        this.available = true
        this.publish({
            type: 'naturalize',
            id: this.legislator.id,
            location: this.location
        }, true, async())
    })
})

Kibitzer.prototype.dispatch = cadence(function (async, body) {
    switch (body.type) {
    case 'health':
        return {}
    case 'sync':
        this._sync(body, async())
        break
    case 'receive':
        this._receive(body, async())
        break
    case 'enqueue':
        this._enqueue(body, async())
        break
    }
})

Kibitzer.prototype.publish = cadence(function (async, entry, internal) {
    var cookie = this.client.publish(entry, internal)
    this.cookies[cookie] = async()
    this._reactor.check()
    return cookie
})

Kibitzer.prototype._enqueue = cadence(function (async, post) {
    if (!this.available) {
        this._logger('info', 'enqueue', {
            kibitzerId: this.legislator.id,
            available: this.available,
            received: post
        })
        throw interrupt(new Error('unavailable'))
    }
    var response = { posted: false, entries: [] }
    post.entries.forEach(function (entry) {
        var outcome = this.legislator.post(this._Date.now(), entry.cookie, entry.value, entry.internal)
        // TODO I expect the cookie to be in the outcome, it's not there.
        // TODO Test receiving entries, enqueuing, when we are not the leader.
        if (outcome.posted) {
            response.posted = true
            response.entries.push({ cookie: entry.cookie, promise: outcome.promise })
        }
    }, this)
    this._reactor.check()
    this._logger('info', 'enqueue', {
        kibitzerId: this.legislator.id,
        available: this.available,
        received: JSON.stringify(post),
        sent: JSON.stringify(response)
    })
    return response
})

Kibitzer.prototype._receive = cadence(function (async, post) {
    if (!this.available) {
        this._logger('info', 'receive', {
            kibitzerId: this.legislator.id,
            available: this.available,
            received: post
        })
        throw interrupt(new Error('unavailable'))
    }
    var route = post.route, index = post.index, expanded = post.messages
    async(function () {
        route = this.legislator.routeOf(route.path, route.pulse)
        this.legislator.inbox(this._Date.now(), route, expanded)
        if (index + 1 < route.path.length) {
            var serialized
            async(function () {
                var forwards = this.legislator.forwards(this._Date.now(), route, index)
                serialized = {
                    type: 'receive',
                    route: route,
                    index: index + 1,
                    messages: forwards
                }
                var location = this.legislator.locations[route.path[index + 1]]
                this._ua.send(location, serialized, async())
            }, function (body, response) {
                assert.ok(body.returns)
                var returns = body.returns
                this._logger('info', 'published', {
                    kibitzerId: this.legislator.id,
                    sent: serialized,
                    received: body
                })
                this.legislator.inbox(this._Date.now(), route, returns)
            })
        }
    }, function () {
        var returns = this.legislator.returns(this._Date.now(), route, index)
        this._reactor.check()
        this._logger('info', 'receive', {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            available: this.available,
            received: post,
            returns: returns
        })
        return { returns: returns }
    })
})

module.exports = Kibitzer
