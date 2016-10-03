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
//
// TODO Here I am, back to remove the EventEmitter. Not sure how anyone is able
// to program that way, treating everything in your application as a stream.
// Pushing log entires as events, hard to reason about this firehose events, and
// it makes any asynchronous calls in response to an event require subsequent
// queuing of events, so why not use this queue that already exists?

// Quality control.
var assert = require('assert')

// EventEmitter API.
var events = require('events')
var util = require('util')

// Varadic arguments.
var slice = [].slice

// Control-flow libraries.
var abend = require('abend')
var cadence = require('cadence')
var Reactor = require('reactor')
var Scheduler = require('happenstance')
var Vestibule = require('vestibule')

// Paxos libraries.
var Legislator = require('paxos/legislator')
var Islander = require('islander')
var Monotonic = require('monotonic').asString

// Logging.
var logger = require('prolific.logger').createLogger('kibitz')

var Notifiers = {
    prime: function () {
        if (this._kibitzer.legislator.id == '2') {
            null
        }
        if (this._kibitzer._prime()) {
            this._kibitzer._notifier = {
                _kibitzer: this._kibitzer,
                notify: Notifiers.advance
            }
            this._kibitzer.emit('enqueued')
        }
    },
    advance: function () {
        if (this._kibitzer.legislator.id == '2') {
            null
        }
        this._kibitzer._advance()
    }
}

var Shifters = {
    null: function () { return null },
    join: function () {
        var entry = this._kibitzer._shift()
        this._kibitzer._shifter = {
            _kibitzer: this._kibitzer,
            _entry: entry,
            shift: Shifters.first
        }
        return { type: 'join', entry: entry.entry }
    },
    first: function () {
        this._kibitzer._shifter = {
            _kibitzer: this._kibitzer,
            shift: Shifters.shift
        }
        return this._entry
    },
    shift: function () {
        return this._kibitzer._shift()
    },
    terminate: function () {
        // TODO `stop`, `shutdown`, `terminate`, `close`, `exit`?
        this._kibitzer._shifter = { shift: Shifters.null }
        this._kibitzer.emit('shutdown')
        return { type: 'terminate' }
    }
}
// The `Kibitzer` object contains an islander, which will submit messages to
// Paxos, track the log generated by Paxos and resubit any messages that might
// have been dropped.

//
function Kibitzer (options) {
    // Set option defaults.
    options || (options = {})

    options.ping || (options.ping = 250)
    options.timeout || (options.timeout = 1000)

    // The HTTP user agent is provided so it can mocked for testing.
    assert(options.ua)
    this._ua = options.ua

    // TODO How are these properties used? It seems like there is a properties
    // object at every level of abstraction. I'd expect the properties object in
    // Paxos to be the definitive properties mechanism. They should be set with
    // an accessor to record the property setting.
    this.properties = options.properties || {}

    // Queues for Paxos messages to send and user messages to submit to Paxos.
    this._sender = new Reactor({ object: this, method: '_send' })
    this._publisher = new Reactor({ object: this, method: '_publish' })

    // Time obtained from optional `Date` for unit testing.
    this._Date = options.Date || Date
    this.scheduler = new Scheduler({ Date: this._Date })

    this.legislator = new Legislator(null, options.kibitzerId, options.cookie || this._Date.now(), {
        ping: options.ping,
        timeout: options.timeout,
        scheduler: {
            Date: options.Date || Date,
            timerless: options.timerless || false
        }
    })

// TODO Scheduler is not shutting down, so maybe `unref` for now.
    this.legislator.scheduler.on('timeout', this._checkOutbox.bind(this))

    // Submission queue with resubmission logic.
    this.islander = new Islander(this.legislator.id)

    // Iterators to advance through both the Paxos and Islander message logs.
    this.iterators = {
        legislator: this.legislator.log.min(),
        islander: { dummy: true }
    }

    // More crufty, but not terribly crufty.
    this._terminated = false

    // Used to record events generated during playback.
    this._recording = { paxos: [], islander: [] }

    events.EventEmitter.call(this)

    // Invoke priming on first notification.
    this._notifier = { notify: Notifiers.prime, _kibitzer: this }
    this._shifter = { shift: Shifters.null }
}
util.inherits(Kibitzer, events.EventEmitter)

// We record events generated during playback so that they can be compared with
// the actual events played back from the log to assert that the playback is
// indeed determinic.

//

// Put the Kibitzer in a replay state. TODO Maybe move this into the
// constructor, or better still, use a decorator pattern.

//
Kibitzer.prototype.replay = function () {
// TODO Can Prolific Logger support this pattern? TODO Actually, just patch over
// the `Writer` with a decorator, document the example as the way to do it. The
// writer is passive, you should only be setting it up in a program, during
// program startup, prior to any logging. There ought to be no logging during
// `require`, but if there is, then you need to concern yourself with the order
// of require, and that okay, if you've got that edge case for true.
    this.legislator._trace = function (method, vargs) {
        this._recording.paxos.push({ method: method, vargs: JSON.parse(JSON.stringify(vargs)) })
    }.bind(this)
    this.islander._trace = function (method, vargs) {
        this._recording.islander.push({ method: method, vargs: JSON.parse(JSON.stringify(vargs)) })
    }.bind(this)
}

// Play an entry in the playback log.

//
Kibitzer.prototype.play = function (entry) {
    if (entry.qualifier == 'paxos') {
        if (this._recording.paxos.length) {
            assert.deepEqual(this._recording.paxos.shift(), {
                method: entry.name,
                vargs: entry.$vargs
            })
        } else {
            this.legislator[entry.name].apply(this.legislator, entry.$vargs)
            this._notifier.notify()
            this.play(entry)
        }
    } else if (entry.qualifier == 'islander') {
        if (this._recording.islander.length) {
            assert.deepEqual(this._recording.islander.shift(), {
                method: entry.name,
                vargs: entry.$vargs
            })
        } else {
            this.islander[entry.name].apply(this.islander, entry.$vargs)
            this.play(entry)
        }
    }
}

Kibitzer.prototype.bootstrap = function (startedAt) {
    this._logger('info', 'bootstrap', { kibitzerId: this.legislator.id })
    this.legislator.islandId = startedAt
    this.legislator.bootstrap(this._Date.now(), this.properties)
    this._checkOutbox()
}

// TODO Use Isochronous to repeatedly send join message.
Kibitzer.prototype.join = cadence(function (async, properties) {
    assert(typeof properties == 'object')
    this.scheduler.schedule(this._Date.now() + 0, 'join', { object: this, method: '_checkJoin' }, properties)
})

// Enqueue a user message into the `Islander`. The `Islander` will submit the
// message, monitor the atomic log, and then resubmit the message if it detects
// that the message was lost.
Kibitzer.prototype.publish = function (entry) {
    var cookie = this.islander.publish(entry, false)
    logger.info('publish', { $entry: entry, cookie: cookie })
    // TODO Not necessary, just check publisher.
    if (this.scheduler.scheduled('publish') == null) {
        this.scheduler.schedule(0, 'publish', { object: this, method: '_checkPublisher' })
    }
    return cookie
}

// Called by your network implementation with messages enqueued from another
// Kibitz.
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

// Stop timers, and stop timers only. We're not in a position to notify clients
// that there will be no more messages.
Kibitzer.prototype.shutdown = function () {
    if (!this._shutdown) {
        this._shutdown = true
        this.scheduler.shutdown()
        this.legislator.scheduler.shutdown()
        this._shifter = { _kibitzer: this, shift: Shifters.terminate }
        this.emit('enqueued')
    }
}

// TODO Priming is no longer necessary with pull interface.
Kibitzer.prototype._prime = function () {
    var primed = false
    if (this.iterators.legislator.next != null) {
        primed = true
        this.iterators.legislator = this.iterators.legislator.next
        this.iterators.islander = {
            next: this.islander.prime(this.iterators.legislator)
        }
        this._shifter = { _kibitzer: this, shift: Shifters.join }
    }
    return primed
}

// When we join we repeatedly submit a immigration request until we know that
// we've immigrated.

// Invoked periodically by the scheduler to see if immigration was successful,
// resubmit if it was not successful.
Kibitzer.prototype._checkJoin = function (when, properties) {
    this._join(properties, abend)
}

Kibitzer.prototype.join = cadence(function (async, leader) {
// TODO Should this be or should this not be? It should be. You're sending your
// enqueue messages until you immigrate. You don't know when that will be.
// You're only going to know if you've succeeded if your legislator has
// immigrated. That's the only way.
    if (this.legislator.government.promise != '0/0') {
        // TODO This should be rejected when you enqueue, it shoudln't matter.
        console.log('Hey! I got a government.')
        return
    }
    // throw new Error
    async(function () {
        this._logger('info', 'join', { kibitzerId: this.legislator.id })
        this.legislator.islandId = leader.islandId
        this._ua.send(leader, {
            type: 'immigrate',
            islandId: leader.islandId,
            id: this.legislator.id,
            cookie: this.legislator.cookie,
            properties: this.properties,
            hops: 0
        }, async())
    }, function (response) {
        return response != null && response.enqueued
    })
})

// TODO Wierd but scheduler calls us with a timing, so that is interpreted as
// the callback to `check`. Could spend all day trying to decide if that's a
// correct use of `Operation`.
Kibitzer.prototype._checkPublisher = function () {
    logger.info('_checkPublisher', { publisher: this._publisher.turnstile.health })
    this._publisher.check()
}

// Internal publishing.
Kibitzer.prototype._publish = cadence(function (async) {
    var loop = async(function () {
        var outgoing = this.islander.outbox()
        logger.info('_publish', { $outgoing: outgoing, $sent: this.islander.sent.ordered })
        if (outgoing.length == 0) {
            return [ loop.break ]
        }
        var post
        async(function () {
            var properties = this.legislator.properties[this.legislator.government.majority[0]]
            this._ua.send(properties, post = {
                islandId: this.legislator.islandId,
                type: 'enqueue',
                entries: outgoing
            }, async())
        }, function (entries) {
            this._logger('info', 'enqueued', {
                kibitzerId: this.legislator.id,
                $post: post,
                $entries: entries
            })
            entries || (entries = [])
            this.islander.published(entries)
            this._notifier.notify()
        })
    })()
})

// TODO Annoying how difficult it is to stop this crazy thing. There are going
// to be race conditions where we have a termination, come in, we shut things
// down, but then we continue with processing a pulse which triggers a timer.
// Sending messages to legislator can restart it's scheduler.
//
// TODO We could kill the timer in the scheduler, set the boolean we added to
// tell it to no longer schedule.
Kibitzer.prototype._send = cadence(function (async, timeout, pulse) {
    var responses = {}
    async(function () {
        async.forEach(function (id) {
            async(function () {
                if (id == this.legislator.id) {
                    this._receive(pulse, async())
                } else {
                    var properties = this.legislator.properties[id]
                    this._ua.send(properties, {
                        type: 'receive',
                        pulse: pulse
                    }, async())
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
        this._notifier.notify()
        this._checkOutbox()
    })
})

Kibitzer.prototype._checkOutbox = function () {
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
            this._sender.push(pulse)
        }, this)
    }
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
        var properties = this.legislator.properties[this.legislator.government.majority[0]]
        post.hops++
        this._ua.send(properties, post, async())
    } else {
        this._checkOutbox()
        return [ outcome ]
    }
})

Kibitzer.prototype._enqueue = cadence(function (async, post) {
    var entries = []
// TODO Redirect enqueue or wait for stability and retry.
    for (var i = 0, I = post.entries.length; i < I; i++) {
        var entry = post.entries[i]
        var outcome = this.legislator.enqueue(this._Date.now(), post.islandId, entry)
        if (!outcome.enqueued) {
            entries.length = 0
            break
        }
        entries.push({ cookie: entry.cookie, promise: outcome.promise })
    }
    this._checkOutbox()
    this._logger('info', 'enqueue', {
        kibitzerId: this.legislator.id,
        $post: post,
        $entries: entries
    })
    return [ entries ]
})

Kibitzer.prototype._receive = cadence(function (async, pulse) {
    this._logger('info', 'receive', {
        kibitzerId: this.legislator.id,
        received: pulse
    })
    var ret = [ this.legislator.receive(this._Date.now(), pulse, pulse.messages) ]
    this._notifier.notify()
    this._checkOutbox()
    this._checkPublisher()
    return ret
})

Kibitzer.prototype._advance = function () {
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
    this.emit('enqueued')
}

Kibitzer.prototype.shift = function () {
    return this._shifter.shift()
}

Kibitzer.prototype._shift = function () {
    var entry = this.iterators.islander.next
    if (entry == null) {
        return null
    }
    this.iterators.islander = entry
// TODO Think hard about how messy this has become.
// TODO Maybe header and body and the header is used internallyish? Islander would use it.
    var government = Monotonic.isBoundary(entry.promise, 0)
    if (entry.value.type == 'government') {
        return {
            type: 'entry',
            entry: {
                promise: entry.promise,
                government: entry.value.government,
                collapsed: entry.value.collapsed,
                properties: entry.value.properties
            }
        }
    }
    return {
        type: 'entry',
        entry: {
            promise: entry.promise,
            value: entry.value.value
        }
    }
}

// TODO Replace with `logger.sink.writer` decorator.
Kibitzer.prototype._logger = function (level, message, context) {
    logger[level](message, context)
}

// TODO Where is this being used? Why not just reference the Legislators
// properties directly?
Kibitzer.prototype.getProperties = function () {
    var properties = []
    for (var key in this.legislator.properties) {
        properties.push(this.legislator.properties[key])
    }
    return properties
}

module.exports = Kibitzer
