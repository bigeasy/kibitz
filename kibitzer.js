// Node.js API.
var assert = require('assert')
var util = require('util')

// Deep diff.
var departure = require('departure')

// Common utiltieis.
var util = require('util')
var nop = require('nop')

// Control-flow libraries.
var cadence = require('cadence')
var Timer = require('happenstance').Timer
var Procession = require('procession')

// Paxos libraries.
var Paxos = require('paxos')
var Islander = require('islander')
var Monotonic = require('monotonic').asString

// Catch exceptions based on a regex match of an error message or property.
var rescue = require('rescue/redux')

// The `Kibitzer` object contains an islander, which will submit messages to
// Paxos, track the log generated by Paxos and resubit any messages that might
// have been dropped.

//
function Kibitzer (options) {
    // These defaults are a bit harsh if you're going to log everything.
    options.ping || (options.ping = 250)
    options.timeout || (options.timeout = 1000)

    // Time obtained from optional `Date` for unit testing.
    this._Date = options.Date || Date

    this._ua = options.ua

    this.paxos = new Paxos(this._Date.now(), options.id, {
        ping: options.ping,
        timeout: options.timeout
    })

    // Submission queue with resubmission logic.
    this.islander = new Islander(options.id)

    this.played = new Procession
}

Kibitzer.prototype.listen = cadence(function (async, destructible) {
    destructible.markDestroyed(this, 'destroyed')

    destructible.destruct.wait(this.paxos.scheduler, 'clear')

    // Paxos also sends messages to Islander for accounting.
    destructible.monitor('islander', this.paxos.log.pump(this.islander, 'push'), 'destructible', null)

    // TODO Pass an "operation" to `Procession.pump`.
    var timer = new Timer(this.paxos.scheduler)
    destructible.monitor('timer', timer.events.pump(this, function (envelope) {
        this.play('event', envelope)
    }), 'destructible', null)
    destructible.monitor('scheduler', this.paxos.scheduler.events.pump(timer, 'enqueue'), 'destructible', null)
    destructible.monitor('publish', this.islander.outbox.pump(this, '_publish'), 'destructible', null)
    /*
    this.islander.outbox.pump(this, '_publish').run(destructible.monitor('publish'))
    destructible.destruct.wait(this, function () {
        this.islander.outbox.push(null)
    })
    */
    destructible.monitor('send', this.paxos.outbox.pump(this, '_send'), 'destructible', null)
    return []
})

// You can just as easily use POSIX time for the `republic`.
Kibitzer.prototype.bootstrap = function (republic, properties) {
    assert(republic != null)
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
        this.paxos.bootstrap(envelope.body.republic, envelope.when, envelope.body.properties)
        break
    case 'join':
        this.paxos.join(envelope.body.republic, envelope.when)
        break
    case 'acclimate':
        this.paxos.acclimate()
        break
    case 'event':
        this.paxos.event(envelope.body)
        break
    case 'embark':
        var body = envelope.body
        return this.paxos.embark(envelope.when, body.republic, body.id, body.cookie, body.properties)
    case 'receive':
        // TODO Split pulse from messages somehow, make them siblings, not nested.
        return this.paxos.request(envelope.when, envelope.body)
    case 'enqueue':
        return this._enqueue(envelope.when, envelope.body)
    case 'publish':
        this.islander.publish(envelope.body)
        break
    case 'published':
        this.islander.sent(envelope.body.cookie, envelope.body.promises)
        break
    case 'sent':
        this.paxos.response(envelope.when, envelope.body.cookie, envelope.body.responses)
        break
    }
}

// TODO You are assuming that an address is not an address but a set of
// properties, so you need to provide those properties for leader as an
// argument, not just a url or identifier.

//
Kibitzer.prototype.join = function (republic, leader, properties) {
// TODO Should this be or should this not be? It should be. You're sending your
// enqueue messages until you arrive. You don't know when that will be. You're
// only going to know if you've succeeded when you've finally arrived. That's
// the only way.

// TODO Was a test, but it is now an assertion and it really ought be an
// exception because it is not impossible.
    assert(this.paxos.government.promise == '0/0')
    assert(republic != null)

    this.play('join', { republic: republic })
}

Kibitzer.prototype.acclimate = function () {
    this.play('acclimate', {})
}

// Publish to consensus algorithm from islander retryable client.
Kibitzer.prototype._publish = cadence(function (async, envelope) {
    if (envelope == null) {
        return
    }
    async([function () {
        var properties = this.paxos.government.properties[this.paxos.government.majority[0]]
        this._ua.send({
            module: 'kibitz',
            method: 'enqueue',
            to: properties,
            body: {
                republic: this.paxos.government.republic,
                entries: envelope.messages
            }
        }, async())
    }, rescue(/^conduit#endOfStream$/m, null)], function (promises) {
        this.play('published', { cookie: envelope.cookie, promises: promises })
    })
})

// TODO Annoying how difficult it is to stop this crazy thing. There are going
// to be race conditions where we have a termination, come in, we shut things
// down, but then we continue with processing a pulse which triggers a timer.
// Sending messages to paxos can restart it's scheduler.
//
// TODO We could kill the timer in the scheduler, set the boolean we added to
// tell it to no longer schedule.
//
// TODO Regarding the above, you need to make sure to destroy the timer as a
// first step using truncate.
//
// TODO This needs to be parallelized.
Kibitzer.prototype._send = cadence(function (async, communique) {
    if (communique == null) {
        return
    }
    var responses = {}
    async(function () {
        communique.envelopes.forEach(function (envelope) {
            async([function () {
                this._ua.send({
                    module: 'kibitz',
                    method: 'receive',
                    to: envelope.properties,
                    body: envelope.request
                }, async())
            }, rescue(/^conduit#endOfStream$/m, null)], function (response) {
                communique.responses[envelope.to] = response
            })
        }, this)
    }, function () {
        this.play('sent', { cookie: communique.cookie, responses: communique.responses })
    })
})

Kibitzer.prototype.embark = function (republic, id, cookie, properties) {
    return this.play('embark', {
        republic: republic,
        id: id,
        cookie: cookie,
        properties: properties
    })
}

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
