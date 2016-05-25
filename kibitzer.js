// TODO I suppose my attitude is to use an event emitter very sparingly, because
// I want to use one to emit Kibitzer events, the paxos log, but I don't want to
// create an interface that looks like one of the EventEmitter heavy interfaces.
//
// One of the challenges here is that events are going to flow immediately,
// unless I do something compliacated like only emit events when there is a
// listener. Having looked at that, it is not actually that complicated.
//
// Now we can fuss about some terminate logic.
//
// Put it out again, I prefer to program with error-first callbacks, but events
// happen. They are generally a way in which information enters the system. They
// are synchronous. Here is more information. Generally, they shouldn't block.
//
// That's how I use EventEmitters, but I'm not in for a penny, in for a pound.
// If there is a source of events, I treat that as a stream of events. Not in
// the Node.js streams sense, but I create an event emitter that emits a
// homogenous series of events terminated by a specific termination event.
//
// Then I stop. I don't go on to implement multi-interfaces that could take an
// error-first callback, or maybe register an event handler, or maybe use some
// other form of asynchronous notification.
//
// TODO Which is why I've made the event emitter in this class a separate
// object. Which is such a good idea, I belive I'll go and do it Happenstance.
var assert = require('assert')
var events = require('events')

var slice = [].slice

var Vestibule = require('vestibule')
var cadence = require('cadence')

var Reactor = require('reactor')
var Legislator = require('paxos/legislator')
var Islander = require('islander')
var Monotonic = require('monotonic').asString

var Scheduler = require('happenstance')

var interrupt = require('interrupt').createInterrupter('bigeasy.kibitz')
var abend = require('abend')

var logger = require('prolific').createLogger('bigasy.kibitz.kibitzer')

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

    // TODO Very much crufty.
    this.log = new events.EventEmitter
    this.log.on('newListener', this._newLogListener.bind(this))

    this._advanced = new Vestibule
    this._terminated = false
}

Kibitzer.prototype.terminate = function () {
    if (!this._terminated) {
        this._terminated = true
        this.scheduler.shutdown()
        this.legislator.scheduler.shutdown()
        this._advanced.notify()
    }
}

Kibitzer.prototype._newLogListener = function () {
    setImmediate(this._advanced.notify.bind(this._advanced))
}

Kibitzer.prototype._logger = function (level, message, context) {
    logger[level](message, context)
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

// TODO Annoying how difficult it is to stop this crazy thing. There are going
// to be race conditions where we have a termination, come in, we shut things
// down, but then we continue with processing a pulse which triggers a timer.
// Sending messages to legislator can restart it's scheduler.
//
// TODO We could kill the timer in the scheduler, set the boolean we added to
// tell it to no longer schedule.
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

Kibitzer.prototype._prime = cadence(function (async) {
    async(function () {
        var loop = async(function () {
            async(function () {
                this._advanced.enter(async())
            }, function () {
                if (this.iterators.legislator.next != null) {
                    this.iterators.legislator = this.iterators.legislator.next
                    this.iterators.islander = {
                        next: this.islander.prime(this.iterators.legislator)
                    }
                    return [ loop.break ]
                }
            })
        })()
    }, function () {
        this._advance(abend)
    })
})

Kibitzer.prototype.bootstrap = cadence(function (async) {
    this._logger('info', 'bootstrap', { kibitzerId: this.legislator.id })
    this._prime(async())
    this.legislator.bootstrap(this._Date.now(), this.location)
    this._send()
})

// TODO Use Isochronous to repeatedly send join message.
Kibitzer.prototype.join = cadence(function (async, location) {
    this._prime(async())
    this.scheduler.schedule(this._Date.now() + 0, 'join', { object: this, method: '_checkJoin' }, location)
})

Kibitzer.prototype._checkJoin = function (when, location) {
    this._join(location, abend)
}

Kibitzer.prototype._join = cadence(function (async, location) {
    async(function () {
        this._logger('info', 'join', {
            kibitzerId: this.legislator.id,
            received: JSON.stringify(location)
        })
        this._ua.send(location, {
            type: 'naturalize',
            islandId: this.legislator.islandId,
            id: this.legislator.id,
            cookie: this.legislator.cookie,
            location: this.location,
            hops: 0
        }, async())
    }, function (response) {
        if (response == null || !response.enqueued) {
            var delay = this._Date.now() + 1000
            this.scheduler.schedule(delay, 'join', { object: this, method: '_checkJoin' }, location)
        }
    })
})

Kibitzer.prototype.dispatch = cadence(function (async, body) {
    switch (body.type) {
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
    assert(post.hops != null)
    var outcome = this.legislator.naturalize(this._Date.now(), post.islandId, post.id, post.cookie, post.location)
    this._logger('info', 'enqueue', {
        kibitzerId: this.legislator.id,
        received: JSON.stringify(post),
        outcome: JSON.stringify(outcome)
    })
    if (!outcome.enqueued && outcome.leader != null && post.hops == 0) {
        var location = this.legislator.locations[this.legislator.government.majority[0]]
        post.hops++
        this._ua.send(location, post, async())
    } else {
        this._send()
        return [ outcome ]
    }
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

Kibitzer.prototype._advance = cadence(function (async) {
    var loop = async(function () {
        if (this._terminated) {
            this.log.emit('terminated')
            return [ loop.break ]
        }
        while (this.iterators.legislator.next != null) {
            this.iterators.legislator = this.iterators.legislator.next
            this._logger('info', 'consuming', {
                kibitzerId: this.legislator.id,
                route: this.iterators.legislator.route,
                promise: this.iterators.legislator.promise,
                value: this.iterators.legislator.value
            })
        }
        if (this.log.listenerCount('entry') == 0 || this.iterators.islander.next == null) {
            this._advanced.enter(async())
        } else {
// TODO Think hard about how messy this has become.
// TODO Maybe header and body and the header is used internallyish? Islander would use it.
            var entry = this.iterators.islander = this.iterators.islander.next
            var value = Monotonic.isBoundary(entry.promise, 0) ? entry.value : entry.value.value
            this.log.emit('entry', { promise: entry.promise, value: value })
        }
    })()
})

module.exports = Kibitzer
