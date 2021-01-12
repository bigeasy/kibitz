// Node.js API.
const assert = require('assert')
const util = require('util')

// Common utiltieis.
const nop = require('nop')
const coalesce = require('extant')

// Control-flow libraries.
const { Timer } = require('happenstance')
const Avenue = require('avenue')

// Paxos libraries.
const Paxos = require('paxos')
const Islander = require('islander')

// The `Kibitzer` object contains an islander, which will submit messages to
// Paxos, track the log generated by Paxos and resubit any messages that might
// have been dropped.

//
class Kibitzer {
    constructor (destructible, options) {
        // Time obtained from optional `Date` for unit testing.
        this._Date = options.Date || Date

        this._ua = options.ua

        this.paxos = new Paxos(this._Date.now(), options.id, {
            ping: coalesce(options.ping, 1000),
            timeout: coalesce(options.timeout, 3000)
        })

        // Submission queue with resubmission logic.
        this.islander = new Islander(options.id, new Avenue())

        this.played = new Avenue

        destructible.destruct(() => this.destroyed = this)

        destructible.destruct(() => this.paxos.scheduler.clear())

        // Paxos also sends messages to Islander for accounting.
        const islander = this.paxos.log.shifter()
        destructible.durable('islander', islander.push(entry => this.islander.push(entry)))
        destructible.destruct(() => islander.destroy())

        // TODO Pass an "operation" to `Avenue.pump`.
        const timer = new Timer(this.paxos.scheduler)
        destructible.destruct(() => timer.destroy())
        this.paxos.scheduler.on('data', data => this.play('event', data))
        const publish = this.islander.outbox.shifter()
        destructible.durable('publish', publish.push(entry => this._publish(entry)))
        destructible.destruct(() => publish.destroy())
        /*
        this.islander.outbox.pump(this, '_publish').run(destructible.monitor('publish'))
        destructible.destruct.wait(this, function () {
            this.islander.outbox.push(null)
        })
        */
        const send = this.paxos.outbox.shifter()
        destructible.durable('send', send.push(entry => this._send(entry)))
        destructible.destruct(() => send.destroy())
    }

    // You can just as easily use POSIX time for the `republic`.
    bootstrap (republic, properties) {
        assert(republic != null)
        this.play('bootstrap', { republic: republic, properties: properties })
    }

    // Enqueue a user message into the `Islander`. The `Islander` will submit
    // the message, monitor the atomic log, and then resubmit the message if it
    // detects that the message was lost.
    publish (entry) {
        this.play('publish', entry)
    }

    // Called by your network implementation with messages enqueued from another
    // Kibitz.
    request (envelope) {
        switch (envelope.method) {
        case 'receive':
            return this.play('receive', envelope.body)
        case 'enqueue':
            return this.play('enqueue', envelope.body)
        }
    }

    play (method, body) {
        const envelope = {
            module: 'kibitz',
            method: method,
            when: this._Date.now(),
            body: body
        }
        return this.replay(envelope)
    }

    replay (envelope) {
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
    join (republic, leader, properties) {
    // TODO Should this be or should this not be? It should be. You're sending
    // your enqueue messages until you arrive. You don't know when that will be.
    // You're only going to know if you've succeeded when you've finally
    // arrived. That's the only way.

    // TODO Was a test, but it is now an assertion and it really ought be an
    // exception because it is not impossible.
        assert(this.paxos.government.promise == '0/0')
        assert(republic != null)

        this.play('join', { republic: republic })
    }

    acclimate () {
        this.play('acclimate', {})
    }

    // Publish to consensus algorithm from islander retryable client.
    async _publish (envelope) {
        if (envelope == null) {
            return
        }
        const properties = this.paxos.government.properties[this.paxos.government.majority[0]]
        if (properties == null) {
            console.log(this.paxos.government)
            process.exit()
        }
        const promises = await this._ua.send({
            module: 'kibitz',
            method: 'enqueue',
            to: properties,
            body: {
                republic: this.paxos.government.republic,
                entries: envelope.messages
            }
        })
        this.play('published', { cookie: envelope.cookie, promises: promises })
    }

    // TODO Annoying how difficult it is to stop this crazy thing. There are
    // going to be race conditions where we have a termination, come in, we shut
    // things down, but then we continue with processing a pulse which triggers
    // a timer. Sending messages to paxos can restart it's scheduler.
    //
    // TODO We could kill the timer in the scheduler, set the boolean we added
    // to tell it to no longer schedule.
    //
    // TODO Regarding the above, you need to make sure to destroy the timer as a
    // first step using truncate.
    //
    // TODO This needs to be parallelized.
    async _send (communique) {
        if (communique == null) {
            return
        }
        const responses = {}
        for (const envelope of communique.envelopes) {
            communique.responses[envelope.to] = await this._ua.send({
                module: 'kibitz',
                method: 'receive',
                to: envelope.properties,
                body: envelope.request
            })
        }
        this.play('sent', { cookie: communique.cookie, responses: communique.responses })
    }

    embark (republic, id, cookie, properties) {
        return this.play('embark', {
            republic: republic,
            id: id,
            cookie: cookie,
            properties: properties
        })
    }

    _enqueue (when, post) {
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
}

module.exports = Kibitzer
