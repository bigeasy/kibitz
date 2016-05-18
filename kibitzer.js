var assert = require('assert')

var slice = [].slice

var Vestibule = require('vestibule')
var signal = require('signal')
var cadence = require('cadence')

var Reactor = require('reactor')
var Legislator = require('paxos/legislator')
var Islander = require('islander')

var Scheduler = require('happenstance')

var Monotonic = require('monotonic')
var interrupt = require('interrupt').createInterrupter('bigeasy.kibitz')

function Kibitzer (islandId, id, options) {
    assert(id != null, 'id is required')

    options || (options = {})

    options.ping || (options.ping = 250)
    options.timeout || (options.timeout = 1000)

    this._pulser = new Reactor({ object: this, method: '_pulse' })
    this._publisher = new Reactor({ object: this, method: '_publish' })

    this._Date = options.Date || Date
    this.scheduler = new Scheduler({ Date: options.Date || Date })

    this.location = options.location
    this._ua = options.ua
    this.id = id

    this.legislator = new Legislator(islandId, id, this._Date.now(), {
        ping: options.ping,
        timeout: options.timeout,
        scheduler: { Date: options.Date || Date }
    })
// TODO Scheduler is not shutting down, so maybe `unref` for now.
    this.legislator.scheduler.on('timeout', this._send.bind(this))

    this.islander = new Islander(this.legislator.id)

    this.iterators = {
        legislator: this.legislator.log.min(),
        islander: { dummy: true }
    }

    this._advanced = new Vestibule
    this._timeout = null
}

Kibitzer.prototype._logger = function (level, message, context) {
    var subscribers = signal.subscribers([ '', 'bigeasy', 'kibitz', 'log' ])
    for (var i = 0, I = subscribers.length; i < I; i++) {
        subscribers[i](this, level, message, context)
    }
}

Kibitzer.prototype._publish = cadence(function (async) {
    var outgoing = this.islander.outbox()
    if (outgoing.length == 0) {
        return
    }
    var post
    async(function () {
        var location = this.legislator.locations[this.legislator.government.majority[0]]
        this._ua.send(location, post = {
            islandId: this.legislator.islandId,
            type: 'enqueue',
            entries: outgoing
        }, async())
    }, function (body) {
        this._logger('info', 'enqueued', {
            kibitzerId: this.legislator.id,
            sent: post,
            received: body
        })
        if (body == null) {
            body = { entries: [] }
        }
        if (body) {
            this.islander.published(body.entries)
        }
        var delay = this._Date.now() + (body.entries == 0 ? 1000 : 0)
        this.scheduler.schedule(delay, 'publish', { object: this, method: '_checkPublisher' })
    })
})

// TODO Wierd but scheduler calls us with a timing, so that is interpreted as
// the callback to `check`. Could spend all day trying to decide if that's a
// correct use of `Operation`.
Kibitzer.prototype._checkPublisher = function () {
    this._publisher.check()
}


Kibitzer.prototype._pulse = cadence(function (async, timeout, pulse) {
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
    }, function () {
        if (this.iterators.legislator.next != null) {
            this._advanced.notify()
        }
    }, function () {
        this._send()
    })
})

Kibitzer.prototype._send = function () {
    var now = this._Date.now()
    for (;;) {
        var outbox = this.legislator.synchronize(now)
        if (outbox.length == 0) {
            var consensus = this.legislator.consensus(now)
            if (consensus != null) {
                outbox.push(consensus)
            }
        }
        if (outbox.length == 0) {
            break
        }
        outbox.forEach(function (pulse) {
            this._pulser.push(pulse)
        }, this)
    }
}

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
        this.legislator.bootstrap(this._Date.now(), this.location)
        this._advanced.enter(async())
        this._send()
    }, function () {
        this._prime()
    })
})

// TODO Use Isochronous to repeatedly send join message.
Kibitzer.prototype.join = cadence(function (async) {
    async(function () {
        this._ua.discover(async())
    }, function (locations) {
        this._logger('info', 'locations', {
            kibitzerId: this.legislator.id,
            received: JSON.stringify(locations)
        })
// TODO Skip this and send naturalize directly until it is accepted.
// TODO Paxos should reject naturalize based on island id.
// TODO Paxos must reject duplicate naturalization messages, skip naturalization
// if the item is already naturalized.
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
// TODO Let Paxos handle concept availability, which is also changed islands.
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
    if (this.scheduler.scheduled('publish') == null) {
        this.scheduler.schedule(0, 'publish', { object: this, method: '_checkPublisher' })
    }
    return cookie
}

Kibitzer.prototype._naturalize = cadence(function (async, post) {
    var outcome = this.legislator.naturalize(this._Date.now(), post.islandId, post.id, post.cookie, post.location)
    this._logger('info', 'enqueue', {
        kibitzerId: this.legislator.id,
        received: JSON.stringify(post),
        sent: JSON.stringify(outcome)
    })
    this._send()
    return outcome
})

Kibitzer.prototype._enqueue = cadence(function (async, post) {
    var response = { posted: false, entries: [] }
// TODO Redirect enqueue or wait for stability and retry.
    for (var i = 0, I = post.entries.length; i < I; i++) {
        var entry = post.entries[i]
        var outcome = this.legislator.enqueue(this._Date.now(), post.islandId, entry)
        if (!outcome.enqueued) {
            response.entries.length = 0
            break
        }
        response.entries.push({ cookie: entry.cookie, promise: outcome.promise })
    }
    this._send()
    this._logger('info', 'enqueue', {
        kibitzerId: this.legislator.id,
        received: JSON.stringify(post),
        sent: JSON.stringify(response)
    })
    return response
})

Kibitzer.prototype._receive = cadence(function (async, pulse) {
    this._logger('info', 'receive', {
        kibitzerId: this.legislator.id,
        received: pulse
    })
    var ret = [ this.legislator.receive(this._Date.now(), pulse, pulse.messages) ]
    if (this.iterators.legislator.next != null) {
        this._advanced.notify()
    }
    this._send()
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
