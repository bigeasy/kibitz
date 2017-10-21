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
var Procession = require('procession')

// Paxos libraries.
var Paxos = require('paxos')
var Islander = require('islander')
var Monotonic = require('monotonic').asString

// Construction notification and destruction.
var Destructible = require('destructible')
var Signal = require('signal')

// Logging.
var logger = require('prolific.logger').createLogger('kibitz')

// Message queue.
var Requester = require('conduit/requester')

// Catch exceptions based on a regex match of an error message or property.
var rescue = require('rescue')

// The `Kibitzer` object contains an islander, which will submit messages to
// Paxos, track the log generated by Paxos and resubit any messages that might
// have been dropped.

//
function Kibitzer (options) {
    // Log used to drain Islander.
    this.log = new Procession

    // These defaults are a bit harsh if you're going to log everything.
    options.ping || (options.ping = 250)
    options.timeout || (options.timeout = 1000)

    // Time obtained from optional `Date` for unit testing.
    this._Date = options.Date || Date

    assert(options.republic != null)
    this.paxos = new Paxos(this._Date.now(), null, options.id, {
        ping: options.ping,
        timeout: options.timeout
    })

    // Submission queue with resubmission logic.
    this.islander = new Islander(options.id)

    // Paxos sends messages to islander.
    this.paxos.log.shifter().pump(this.islander, 'enqueue')

    this._shifters = null

    // Requesters to make network requests.
    this._requester = new Requester

    this.read = this._requester.read
    this.write = this._requester.write

    this.played = new Procession

    this.islander.log.shifter().pump(this.log, 'enqueue')

    this._destructible = new Destructible(1000, 'kibitzer')
    this._destructible.markDestroyed(this, 'destroyed')

    this._shifters = {
        paxos: this.paxos.outbox.shifter(),
        islander: this.islander.outbox.shifter()
    }

    this._destructible.addDestructor('publish', this._shifters.islander, 'destroy')
    this._destructible.addDestructor('send', this._shifters.paxos, 'destroy')

    this._destructible.addDestructor('scheduler', this.paxos.scheduler, 'clear')
    this._destructible.addDestructor('eos', this.write, 'push')

    this.destruction = this._destructible.events

    this.ready = new Signal
}

Kibitzer.prototype.listen = cadence(function (async) {
    // TODO Pass an "operation" to `Procession.pump`.
    var timer = new Timer(this.paxos.scheduler)
    timer.events.shifter().pump(function (envelope) { this.play('event', envelope) }.bind(this))
    this.paxos.scheduler.events.shifter().pump(timer, 'enqueue')
    this._publish(this._destructible.monitor('publish'))
    this._send(this._destructible.monitor('send'))
    this.ready.unlatch()
    this._destructible.completed.wait(async())
})

// You can just as easily use POSIX time for the `republic`.
Kibitzer.prototype.bootstrap = function (republic, properties) {
    this.play('bootstrap', { republic: republic, properties: properties })
}

// Enqueue a user message into the `Islander`. The `Islander` will submit the
// message, monitor the atomic log, and then resubmit the message if it detects
// that the message was lost.
Kibitzer.prototype.publish = function (entry) {
    this.play('publish', entry)
}

// Called by your network implementation with messages enqueued from another
// Kibitz.
Kibitzer.prototype.request = cadence(function (async, envelope) {
    switch (envelope.method) {
    case 'immigrate':
        this._immigrate(envelope.body, async())
        break
    case 'receive':
        return [ this.play('receive', envelope.body) ]
    case 'enqueue':
        return [ this.play('enqueue', envelope.body) ]
    }
})

Kibitzer.prototype.play = function (method, body) {
    var envelope = {
        module: 'kibitz',
        method: method,
        when: this._Date.now(),
        body: body
    }
    return this.replay(envelope)
}


Kibitzer.prototype.replay = function (envelope) {
    this.played.push(envelope)
    switch (envelope.method) {
    case 'bootstrap':
        this.paxos.republic = envelope.republic
        this.paxos.bootstrap(envelope.when, envelope.body.properties)
        break
    case 'join':
        this.paxos.republic = envelope.republic
        break
    case 'naturalize':
        this.paxos.naturalize()
        break
    case 'event':
        this.paxos.event(envelope.body)
        break
    case 'immigrate':
        var body = envelope.body
        return this.paxos.immigrate(envelope.when, body.republic, body.id, body.cookie, body.properties)
    case 'receive':
        // TODO Split pulse from messages somehow, make them siblings, not nested.
        return this.paxos.request(envelope.when, envelope.body)
    case 'enqueue':
        return this._enqueue(envelope.when, envelope.body)
    case 'publish':
        this.islander.publish(envelope.body)
        break
    case 'published':
        this.islander.sent(envelope.body.promises)
        break
    case 'sent':
        this.paxos.response(envelope.when, envelope.body.cookie, envelope.body.responses)
        break
    }
}

// Stop timers, and stop timers only. We're not in a position to notify clients
// that there will be no more messages.
Kibitzer.prototype.destroy = function () {
    this._destructible.destroy()
}

Kibitzer.prototype.join = cadence(function (async, republic, leader, properties) {
// TODO Should this be or should this not be? It should be. You're sending your
// enqueue messages until you immigrate. You don't know when that will be.
// You're only going to know if you've succeeded if your legislator has
// immigrated. That's the only way.

// TODO Was a test, but it is now an assertion and it really ought be an
// exception because it is not impossible.
    assert(this.paxos.government.promise == '0/0', 'already have government')
    // throw new Error
    async(function () {
        this.play('join', { republic: republic })
        // Note that we're passing properties so that they're logged for
        // inspection during debugging replay, but they're not going to be used
        // as an argument to Paxos on this side. We give them to our leader when
        // we request immigration.
        this._requester.request({
            module: 'kibitz',
            method: 'immigrate',
            to: leader,
            body: {
                republic: this.paxos.republic,
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

Kibitzer.prototype.naturalize = function () {
    this.play('naturalize', {})
}

// Publish to consensus algorithm from islander retryable client.
Kibitzer.prototype._publish = cadence(function (async) {
    var loop = async(function () {
        this._shifters.islander.dequeue(async())
    }, function (messages) {
        if (messages == null) {
            return [ loop.break ]
        }
        async([function () {
            var properties = this.paxos.government.properties[this.paxos.government.majority[0]]
            this._requester.request({
                module: 'kibitz',
                method: 'enqueue',
                to: properties,
                body: {
                    republic: this.paxos.republic,
                    entries: messages
                }
            }, async())
        }, rescue(/^conduit#endOfStream$/m, null)], function (promises) {
            this.play('published', { promises: promises })
        })
    })()
})

// TODO Annoying how difficult it is to stop this crazy thing. There are going
// to be race conditions where we have a termination, come in, we shut things
// down, but then we continue with processing a pulse which triggers a timer.
// Sending messages to paxos can restart it's scheduler.
//
// TODO We could kill the timer in the scheduler, set the boolean we added to
// tell it to no longer schedule.
Kibitzer.prototype._send = cadence(function (async) {
    var loop = async(function () {
        this._shifters.paxos.dequeue(async())
    }, function (communique) {
        if (communique == null) {
            return [ loop.break ]
        }
        var responses = {}
        async(function () {
            communique.envelopes.forEach(function (envelope) {
                async([function () {
                    if (envelope.to == this.paxos.id) {
                        this.request({ method: 'receive', body: envelope.request }, async())
                    } else {
                        this._requester.request({
                            module: 'kibitz',
                            method: 'receive',
                            to: envelope.properties,
                            body: envelope.request
                        }, async())
                    }
                }, rescue(/^conduit#endOfStream$/m, null)], function (response) {
                    communique.responses[envelope.to] = response
                })
            }, this)
        }, function () {
            this.play('sent', { cookie: communique.cookie, responses: communique.responses })
        })
    })()
})

// TODO Hopping is a second way of doing a thing and we don't need a second way
// of doing a thing.
Kibitzer.prototype._immigrate = cadence(function (async, post) {
    async(function () {
        assert(post.hops != null)
        var outcome = this.play('immigrate', post)
        return outcome
    }, function (outcome) {
        if (!outcome.enqueued && outcome.leader != null && post.hops == 0) {
            var properties = this.paxos.government.properties[outcome.leader]
            post.hops++
            this._requester.request({
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
            promises = null
            break
        }
        promises[entry.cookie] = outcome.promise
    }
    return promises
}

module.exports = Kibitzer
