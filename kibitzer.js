var assert = require('assert')
var util = require('util')

var slice = [].slice

var Vestibule = require('vestibule')
var signal = require('signal')
var cadence = require('cadence')

var Reactor = require('reactor')
var Legislator = require('paxos/legislator')
var Islander = require('islander')

var Monotonic = require('monotonic')
var interrupt = require('interrupt').createInterrupter('bigeasy.kibitz')

function Kibitzer (islandId, id, options) {
    assert(id != null, 'id is required')

    options || (options = {})

    options.ping || (options.ping = 250)
    options.timeout || (options.timeout = 1000)

    this.poll = options.poll || 5000
    this.ping = options.ping
    this.timeout = options.timeout

    this.syncLength = options.syncLength || 250

    this._reactor = new Reactor({ object: this, method: '_tick' })

    this._Date = options.Date || Date

    this.joining = []

    this.location = options.location
    this._ua = options.ua

// TODO Maybe have an auto-incrementing id as before.
    this.id = id

    this.legislator = new Legislator(islandId, id, this._Date.now(), {
        ping: this.ping,
        timeout: this.timeout
    })
    this.islander = new Islander(this.legislator.id)
    this.iterators = {
        legislator: this.legislator.log.min(),
        islander: { dummy: true }
    }
    this.cookies = {}

    this.discovery = options.discovery

    this.available = false

    this._advanced = new Vestibule
}

Kibitzer.prototype._logger = function (level, message, context) {
    var subscribers = signal.subscribers([ '', 'bigeasy', 'kibitz', 'log' ])
    for (var i = 0, I = subscribers.length; i < I; i++) {
        subscribers[i](this, level, message, context)
    }
}

Kibitzer.prototype._createLegislator = function (now) {
}

Kibitzer.prototype._tick = cadence(function (async) {
    var dirty = false
    async(function () {
        var outgoing = this.islander.outbox()
        if (outgoing.length) {
            var post
            async(function () {
                var location = this.legislator.locations[this.legislator.government.majority[0]]
                this._ua.send(location, post = {
                    type: 'enqueue',
                    entries: outgoing
                }, async())
            }, function (body) {
                assert.ok('---->', body.entries)
                this._logger('info', 'enqueued', {
                    kibitzerId: this.legislator.id,
                    sent: post,
                    received: body
                })
                this.islander.published(body.entries)
            })
            dirty = true
        }
    }, function () {
        var now = this._Date.now()
        var outbox = this.legislator.synchronize(now)
        if (outbox.length == 0) {
            var consensus = this.legislator.consensus(now)
            if (consensus != null) {
                outbox.push(consensus)
            }
        }
// TODO Put this in another outgoing queue.
        async.forEach(function (pulse) {
            dirty = true
            var responses = {}
            async(function () {
                async.forEach(function (id) {
                    async(function () {
                        if (id == this.legislator.id) {
                            this._receive(pulse, async())
                        } else {
                            var location = this.legislator.locations[id]
                            this._ua.send(location, { type: 'receive', pulse: pulse }, async())
                        }
                    }, function (response) {
                        this._logger('info', 'published', {
                            kibitzerId: this.legislator.id,
                            sent: pulse,
                            received: response
                        })
                        responses[id] = response
                    })
                })(pulse.route)
            }, function () {
                this.legislator.sent(this._Date.now(), pulse, responses)
            })
        })(outbox)
    }, function () {
        if (this.iterators.legislator.next != null) {
        /*
            this.iterators.legislator = this.iterators.legislator.next
            */
            this._advanced.notify()
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

Kibitzer.prototype._prime = function (entry) {
    this.iterators.islander = {
        next: this.islander.prime(this.iterators.legislator = this.iterators.legislator.next)
    }
}

Kibitzer.prototype.bootstrap = cadence(function (async) {
    async(function () {
        this._logger('info', 'bootstrap', { kibitzerId: this.legislator.id })
        this.bootstrapped = true
        this._reactor.turnstile.turnstiles = 1
        this.legislator.bootstrap(this._Date.now(), this.location)
        this.available = true
        this._advanced.enter(async())
        this._reactor.check()
    }, function () {
        this._prime()
    })
})

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
        var sent
        async(function () {
            this._ua.send(location, sent = {
                type: 'naturalize',
                islandId: this.legislator.islandId,
                id: this.legislator.id,
                cookie: this.legislator.cookie,
                location: this.location
            }, async())
        }, function (outcome) {
            this._logger('info', 'join', {
                kibitzerId: this.legislator.id,
                sent: JSON.stringify(sent),
                received: JSON.stringify(outcome)
            })
        })
        this._advanced.enter(async())
    }, function () {
        this.bootstrapped = false
// TODO Let Paxos handle concept availability, which is also changed islands.
        this.available = true
        this._prime()
    })
})

Kibitzer.prototype.dispatch = cadence(function (async, body) {
    switch (body.type) {
    case 'health':
        return {}
    case 'naturalize':
        this._naturalize(body, async())
        break
    case 'receive':
        this._receive(body.pulse, async())
        break
    case 'enqueue':
        this._enqueue(body, async())
        break
    }
})

Kibitzer.prototype.publish = function (entry) {
    var cookie = this.islander.publish(entry, false)
    this._reactor.check()
    return cookie
}

Kibitzer.prototype._naturalize = cadence(function (async, post) {
// TODO Available ever so dubious.
    if (!this.available) {
        this._logger('info', 'enqueue', {
            kibitzerId: this.legislator.id,
            available: this.available,
            received: JSON.stringify(post)
        })
        throw interrupt(new Error('unavailable'))
    }
    if (post.islandId != this.legislator.islandId) {
        throw interrupt(new Error('wrongIsland'))
    }
    var outcome = this.legislator.naturalize(this._Date.now(), post.id, post.cookie, post.location)
    this._logger('info', 'enqueue', {
        kibitzerId: this.legislator.id,
        available: this.available,
        received: JSON.stringify(post),
        sent: JSON.stringify(outcome)
    })
    return outcome
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
// TODO Redirect enqueue or wait for stability and retry.
// TODO Singular.
    post.entries.forEach(function (entry) {
        var outcome = this.legislator.enqueue(this._Date.now(), entry)
        if (outcome.enqueued) {
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

Kibitzer.prototype._receive = cadence(function (async, pulse) {
    this._logger('info', 'receive', {
        kibitzerId: this.legislator.id,
        available: this.available,
        received: pulse
    })
    var ret = [ this.legislator.receive(this._Date.now(), pulse, pulse.messages) ]
    this._reactor.check()
    return ret
})

Kibitzer.prototype.advance = cadence(function (async) {
    var loop = async(function () {
        while (this.iterators.legislator.next != null) {
            this.iterators.legislator = this.iterators.legislator.next
            this._logger('info', 'consuming', {
                kibitzerId: this.legislator.id,
                route: this.iterators.legislator.route,
                promise: this.iterators.legislator.promise,
                value: this.iterators.legislator.value
            })
        }
        if (this.iterators.islander.next == null) {
            this._advanced.enter(async())
        } else {
            this.iterators.islander = this.iterators.islander.next
            return [ loop.break, this.iterators.islander ]
        }
    })()
})

module.exports = Kibitzer
