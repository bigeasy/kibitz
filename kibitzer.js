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

var abend = require('abend')

var logger = require('prolific.logger').createLogger('bigeasy.kibitz.kibitzer')

function Kibitzer (islandId, id, options) {
    assert(id != null, 'id is required')

    options || (options = {})

    options.ping || (options.ping = 250)
    options.timeout || (options.timeout = 1000)

    this._pulser = new Reactor({ object: this, method: '_pulse' })
    this._publisher = new Reactor({ object: this, method: '_publish' })

    this._Date = options.Date || Date
    this.scheduler = new Scheduler({ Date: options.Date || Date })

    this.properties = options.properties
    this._ua = options.ua
    this.id = id

    this.legislator = new Legislator(islandId, id, options.cookie || this._Date.now(), {
        ping: options.ping,
        timeout: options.timeout,
        scheduler: {
            Date: options.Date || Date,
            timerless: options.timerless || false
        }
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
    this._recorded = null
}

Kibitzer.prototype.play = function (entry) {
    if (entry.context == 'bigeasy.paxos') {
        if (this._recorded.length) {
            assert.deepEqual(this._recorded.shift(), {
                method: entry.name,
                vargs: entry.specific.vargs
            })
        } else {
            this.legislator[entry.name].apply(this.legislator, entry.specific.vargs)
            this._advanced.notify()
            this.play(entry)
        }
    }
}

Kibitzer.prototype.replay = function () {
    this._recorded = []
    this.legislator._trace = function (method, vargs) {
        this._recorded.push({ method: method, vargs: JSON.parse(JSON.stringify(vargs)) })
    }.bind(this)
    this._prime(abend)
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
    logger.info('_publish', { outgoing: outgoing })
    if (outgoing.length == 0) {
        return
    }
    var post
    async(function () {
        var properties = this.legislator.properties[this.legislator.government.majority[0]]
        this._ua.send(properties, post = {
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
        this.islander.published(body.entries)
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
                    var properties = this.legislator.properties[id]
                    this._ua.send(properties, { type: 'receive', pulse: pulse }, async())
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

Kibitzer.prototype.getProperties = function () {
    var properties = []
    for (var key in this.legislator.properties) {
        properties.push(this.legislator.properties[key])
    }
    return properties
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
    this.legislator.bootstrap(this._Date.now(), this.properties)
    this._send()
})

// TODO Use Isochronous to repeatedly send join message.
Kibitzer.prototype.join = cadence(function (async, properties) {
    assert(typeof properties == 'object')
    this._prime(async())
    this.scheduler.schedule(this._Date.now() + 0, 'join', { object: this, method: '_checkJoin' }, properties)
})

Kibitzer.prototype._checkJoin = function (when, properties) {
    this._join(properties, abend)
}

Kibitzer.prototype._join = cadence(function (async, properties) {
    async(function () {
        this._logger('info', 'join', {
            kibitzerId: this.legislator.id,
            received: JSON.stringify(properties)
        })
        this._ua.send(properties, {
            type: 'immigrate',
            islandId: this.legislator.islandId,
            id: this.legislator.id,
            cookie: this.legislator.cookie,
            properties: this.properties,
            hops: 0
        }, async())
    }, function (response) {
        if (response == null || !response.enqueued) {
            var delay = this._Date.now() + 1000
            this.scheduler.schedule(delay, 'join', { object: this, method: '_checkJoin' }, properties)
        }
    })
})

Kibitzer.prototype.dispatch = cadence(function (async, body) {
    switch (body.type) {
    case 'immigrate':
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
    logger.info('publish', { entry: entry, cookie: cookie })
    if (this.scheduler.scheduled('publish') == null) {
        this.scheduler.schedule(0, 'publish', { object: this, method: '_checkPublisher' })
    }
    return cookie
}

Kibitzer.prototype._naturalize = cadence(function (async, post) {
    assert(post.hops != null)
    var outcome = this.legislator.immigrate(this._Date.now(), post.islandId, post.id, post.cookie, post.properties)
    this._logger('info', 'enqueue', {
        kibitzerId: this.legislator.id,
        received: JSON.stringify(post),
        outcome: JSON.stringify(outcome)
    })
    if (!outcome.enqueued && outcome.leader != null && post.hops == 0) {
        var properties = this.legislator.citizens[this.legislator.government.majority[0]]
        post.hops++
        this._ua.send(properties, post, async())
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
    this._checkPublisher()
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
            this.islander.receive([ this.iterators.legislator ])
        }
        if (this.log.listenerCount('entry') == 0 || this.iterators.islander.next == null) {
            this._advanced.enter(async())
        } else {
// TODO Think hard about how messy this has become.
// TODO Maybe header and body and the header is used internallyish? Islander would use it.
            var entry = this.iterators.islander = this.iterators.islander.next
            var government = Monotonic.isBoundary(entry.promise, 0)
            var value = government ? entry.value : entry.value.value
            this.log.emit('entry', {
                government: government,
                promise: entry.promise,
                value: value
            })
        }
    })()
})

module.exports = Kibitzer
