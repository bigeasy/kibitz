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
var departure = require('departure')

// Common utiltieis.
var util = require('util')
var nop = require('nop')

// Control-flow libraries.
var abend = require('abend')
var cadence = require('cadence')
var Timer = require('happenstance').Timer
var Queue = require('procession')

// Paxos libraries.
var Paxos = require('paxos')
var Islander = require('islander')
var Monotonic = require('monotonic').asString

var Destructor = require('destructible')

// Logging.
var logger = require('prolific.logger').createLogger('kibitz')

// Message queue.
var Requester = require('conduit/requester')

// The `Kibitzer` object contains an islander, which will submit messages to
// Paxos, track the log generated by Paxos and resubit any messages that might
// have been dropped.

//
function Kibitzer (options) {
    // Log used to drain Islander.
    this.log = new Queue

    // Set option defaults.
    options || (options = {})

    options.ping || (options.ping = 250)
    options.timeout || (options.timeout = 1000)

    // Time obtained from optional `Date` for unit testing.
    this._Date = options.Date || Date

    this.paxos = new Paxos(options.id, {
        ping: options.ping,
        timeout: options.timeout
    })

    // Submission queue with resubmission logic.
    this.islander = new Islander(options.id)

    // Paxos sends messages to islander.
    this.paxos.log.shifter().pump(this.islander)

    this._shifters = null

    // Requesters to make network requests.
    this._requester = new Requester('kibitz')
    this.spigot = this._requester.spigot

    this.played = new Queue

    this.islander.log.pump(this.log)

    this._destructor = new Destructor
    this._destructor.markDestroyed(this, 'destroyed')
    this._destructor.addDestructor('scheduler', this.paxos.scheduler.clear.bind(this.paxos.scheduler))

    this.destruction = this._destructor.events
}

Kibitzer.prototype.listen = cadence(function (async) {
    // TODO Pass an "operation" to `Procession.pump`.
    var timer = new Timer(this.paxos.scheduler)
    timer.events.pump(function (envelope) {
        this.play('event', envelope, abend)
    }.bind(this))
    this.paxos.scheduler.events.pump(timer)
    this._shifters = {
        paxos: this.paxos.outbox.shifter(),
        islander: this.islander.outbox.shifter()
    }
    this._publish(async())
    this._send(async())
})

// You can just as easily use POSIX time for the `republic`.
Kibitzer.prototype.bootstrap = function (republic, properties) {
    this.play('bootstrap', { republic: republic, properties: properties }, abend)
}

// Enqueue a user message into the `Islander`. The `Islander` will submit the
// message, monitor the atomic log, and then resubmit the message if it detects
// that the message was lost.
Kibitzer.prototype.publish = function (entry) {
    this.play('publish', entry, abend)
}

// Called by your network implementation with messages enqueued from another
// Kibitz.
Kibitzer.prototype.request = cadence(function (async, envelope) {
    switch (envelope.method) {
    case 'immigrate':
        this._immigrate(envelope.body, async())
        break
    case 'receive':
        this.play('receive', envelope.body, async())
        break
    case 'enqueue':
        this.play('enqueue', envelope.body, async())
        break
    }
})

Kibitzer.prototype.play = function (method, body, callback) {
    var envelope = {
        module: 'kibitz',
        method: method,
        when: this._Date.now(),
        body: body
    }
    this.replay(envelope, callback)
}

Kibitzer.prototype.replay = cadence(function (async, envelope) {
    this.played.push(envelope)
    switch (envelope.method) {
    case 'bootstrap':
        this.paxos.bootstrap(envelope.when, envelope.body.republic, envelope.body.properties)
        break
    case 'join':
        this.paxos.join(envelope.when, envelope.body.republic)
        break
    case 'event':
        this.paxos.event(envelope.body)
        break
    case 'immigrate':
        var body = envelope.body
        return [ this.paxos.immigrate(envelope.when, body.republic, body.id, body.cookie, body.properties) ]
        break
    case 'receive':
        // TODO Split pulse from messages somehow, make them siblings, not nested.
        return [ this.paxos.receive(envelope.when, envelope.body, envelope.body.messages) ]
        break
    case 'enqueue':
        return [ this._enqueue(envelope.when, envelope.body) ]
    case 'publish':
        this.islander.publish(envelope.body)
        break
    case 'published':
        this.islander.sent(envelope.body.promises)
        break
    case 'sent':
        this.paxos.sent(envelope.when, envelope.body.pulse, envelope.body.responses)
        break
    }
})

// Stop timers, and stop timers only. We're not in a position to notify clients
// that there will be no more messages.
Kibitzer.prototype.destroy = function () {
    this._destructor.destroy()
}

Kibitzer.prototype.join = cadence(function (async, leader, properties) {
// TODO Should this be or should this not be? It should be. You're sending your
// enqueue messages until you immigrate. You don't know when that will be.
// You're only going to know if you've succeeded if your legislator has
// immigrated. That's the only way.
    if (this.paxos.government.promise != '0/0') {
        // TODO This should be rejected when you enqueue, it shoudln't matter.
        console.log('Hey! I got a government.')
        return
    }
    // throw new Error
    async(function () {
        this.play('join', { republic: leader.republic  }, async())
        this._requester.request('kibitz', {
            module: 'kibitz',
            method: 'immigrate',
            to: leader,
            body: {
                republic: leader.republic,
                id: this.paxos.id,
                cookie: this.paxos.cookie,
                properties: properties,
                hops: 0
            }
        }, async())
    }, function (response) {
        return response != null && response.enqueued
    })
})

// Publish to consensus algorithm from islander retryable client.
Kibitzer.prototype._publish = cadence(function (async) {
    this._destructor.async(async, 'publish')(function () {
        this._destructor.addDestructor('publish', this._shifters.islander.destroy.bind(this._shifters.islander))
        var loop = async(function () {
            this._shifters.islander.dequeue(async())
        }, function (messages) {
            if (messages == null) {
                return [ loop.break ]
            }
            async(function () {
                var properties = this.paxos.government.properties[this.paxos.government.majority[0]]
                this._requester.request('kibitz', {
                    module: 'kibitz',
                    method: 'enqueue',
                    to: properties,
                    body: {
                        republic: this.paxos.republic,
                        entries: messages
                    }
                }, async())
            }, function (promises) {
                this.play('published', { promises: promises }, async())
            })
        })()
    })
})

// TODO Annoying how difficult it is to stop this crazy thing. There are going
// to be race conditions where we have a termination, come in, we shut things
// down, but then we continue with processing a pulse which triggers a timer.
// Sending messages to paxos can restart it's scheduler.
//
// TODO We could kill the timer in the scheduler, set the boolean we added to
// tell it to no longer schedule.
Kibitzer.prototype._send = cadence(function (async) {
    this._destructor.async(async, 'send')(function () {
        this._destructor.addDestructor('send', this._shifters.paxos.destroy.bind(this._shifters.paxos))
        var loop = async(function () {
            this._shifters.paxos.dequeue(async())
        }, function (pulse) {
            if (pulse == null) {
                return [ loop.break ]
            }
            var responses = []
            async(function () {
                pulse.route.forEach(function (id) {
                    async(function () {
                        if (id == this.paxos.id) {
                            this.request({ method: 'receive', body: pulse }, async())
                        } else {
                            var properties = this.paxos.government.properties[id]
                            this._requester.request('kibitz', {
                                module: 'kibitz',
                                method: 'receive',
                                to: properties,
                                body: pulse
                            }, async())
                        }
                    }, function (response) {
                        responses[id] = response
                    })
                }, this)
            }, function () {
                this.play('sent', { pulse: pulse, responses: responses }, async())
            })
        })()
    })
})

Kibitzer.prototype._immigrate = cadence(function (async, post) {
    async(function () {
        assert(post.hops != null)
        this.play('immigrate', post, async())
    }, function (outcome) {
        if (!outcome.enqueued && outcome.leader != null && post.hops == 0) {
            var properties = this.paxos.government.properties[outcome.leader]
            post.hops++
            this._requester.request('kibitz', {
                module: 'kibtiz',
                method: 'immigrate',
                to: properties,
                body: post
            }, async())
        } else {
            return [ outcome ]
        }
    })
})

Kibitzer.prototype._enqueue = function (when, post) {
    var promises = {}
    for (var i = 0, I = post.entries.length; i < I; i++) {
        var entry = post.entries[i]
        var outcome = this.paxos.enqueue(when, post.republic, entry)
        if (!outcome.enqueued) {
            entries = null
            break
        }
        promises[entry.cookie] = outcome.promise
    }
    return promises
}

Kibitzer.prototype._receive = cadence(function (async, pulse, when) {
    return [ this.paxos.receive(when, pulse, pulse.messages) ]
})

module.exports = Kibitzer
