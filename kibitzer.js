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

var util = require('util')

// Varadic arguments.
var slice = [].slice

// Control-flow libraries.
var abend = require('abend')
var cadence = require('cadence')
var Reactor = require('reactor')
var Scheduler = require('happenstance')
var Queue = require('procession')
var Sequester = require('sequester')

// Paxos libraries.
var Paxos = require('paxos/legislator')
var Islander = require('islander')
var Monotonic = require('monotonic').asString

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

    // TODO How are these properties used? It seems like there is a properties
    // object at every level of abstraction. I'd expect the properties object in
    // Paxos to be the definitive properties mechanism. They should be set with
    // an accessor to record the property setting.
    this.properties = options.properties || {}

    // Time obtained from optional `Date` for unit testing.
    this._Date = options.Date || Date
    this.scheduler = new Scheduler({ Date: this._Date })

    this.paxos = new Paxos(options.id, {
        ping: options.ping,
        timeout: options.timeout,
        scheduler: {
            Date: options.Date || Date,
            timerless: options.replaying
        }
    })

    // Submission queue with resubmission logic.
    this._islander = new Islander(options.id)

    // Paxos sends messages to islander.
    this.paxos.log.shifter().pump(this._islander)

    // While we read messages off of the Islander, uh, but it is not necessary!?!
    this.shifter = this._islander.log.shifter()

    // Used to record events generated during playback.
    this._recording = { paxos: [], islander: [] }

    this._messengers = { legislator: new Queue, islander: new Queue }

    this.paxos.outbox.shifter().pump(this._messengers.legislator)
    this._islander.outbox.shifter().pump(this._messengers.islander)

    this._outboxes = {
        legislator: this._messengers.legislator.shifter(),
        islander: this._messengers.islander.shifter()
    }

    // Requesters to make network requests.
    this._requester = new Requester('kibitz')
    this.spigot = this._requester.spigot

    this._islander.log.pump(this.log)

    this._publish(abend)
    this._send(abend)

    this._pumping = Sequester.createLock()
    this._pumping.share(function () {})
    this._pumping.share(function () {})
}

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
    var sink = require('prolific.logger').sink
    var writer = sink.writer
    var recording = this._recording
    sink.writer = {
        write: function (buffer) {
            var entry = JSON.parse(buffer.toString())
            switch (entry.qualifier) {
            case 'kibitz':
                break
            case 'paxos':
            case 'islander':
                recording[entry.qualifier].push({
                    method: entry.name,
                    vargs: entry.$vargs
                })
                break
            }
            writer.write(buffer)
        }
    }
}

Kibitzer.prototype._play_paxos = function (entry, callback) {
    this.legislator[entry.name].apply(this.legislator, entry.$vargs)
    this._notifier.notify()
    this.play(entry, callback)
}

Kibitzer.prototype._play_islander = function (entry, callback) {
    this.islander[entry.name].apply(this.islander, entry.$vargs)
    this._notifier.notify()
    this.play(entry, callback)
}

// Play an entry in the playback log.

//
var waiting = false
Kibitzer.prototype.play = cadence(function (async, entry) {
    assert(!waiting)
    switch (entry.qualifier) {
    case 'kibitz':
        if (entry.name == 'shift') {
            async(function () {
                waiting = true
                this._replayed.push({
                    shifted: entry.shifted,
                    callback: async()
                })
                this.emit('enqueued')
            }, function () {
                waiting = false
            })
        }
        break
    case 'paxos':
    case 'islander':
        var recording = this._recording[entry.qualifier]
        if (recording.length) {
            departure.raise({
                method: entry.name,
                vargs: entry.$vargs
            }, recording.shift())
        } else {
            this['_play_' + entry.qualifier](entry, async())
        }
        break
    }
})

// You can just as easily use POSIX time for the `islandId`.
Kibitzer.prototype.bootstrap = function (islandId) {
    this.paxos.bootstrap(this._Date.now(), islandId, this.properties)
}

// Enqueue a user message into the `Islander`. The `Islander` will submit the
// message, monitor the atomic log, and then resubmit the message if it detects
// that the message was lost.
Kibitzer.prototype.publish = function (entry) {
    return this._islander.publish(entry)
}

// Called by your network implementation with messages enqueued from another
// Kibitz.
Kibitzer.prototype.request = cadence(function (async, envelope) {
    switch (envelope.method) {
    case 'immigrate':
        this._immigrate(envelope.body, async())
        break
    case 'receive':
        this._receive(envelope.body, async())
        break
    case 'enqueue':
        this._enqueue(envelope.body, async())
        break
    }
})

// Stop timers, and stop timers only. We're not in a position to notify clients
// that there will be no more messages.
Kibitzer.prototype.shutdown = cadence(function (async) {
    if (!this._shutdown) {
        this._pumping.exclude(async())
        this._outboxes.legislator.destroy()
        this._outboxes.islander.destroy()
        this._shutdown = true
        this.scheduler.shutdown()
        this.paxos.scheduler.shutdown()
    }
})

Kibitzer.prototype.join = cadence(function (async, leader) {
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
        this.paxos.join(this._Date.now(), leader.islandId)
        this._requester.request('kibitz', {
            module: 'kibitz',
            method: 'immigrate',
            to: leader,
            body: {
                islandId: leader.islandId,
                id: this.paxos.id,
                cookie: this.paxos.cookie,
                properties: this.properties,
                hops: 0
            }
        }, async())
    }, function (response) {
        return response != null && response.enqueued
    })
})

// Publish to consensus algorithm from islander retryable client.
Kibitzer.prototype._publish = cadence(function (async) {
    async([function () {
        this._pumping.unlock()
    }], function () {
        var loop = async(function () {
            async(function () {
                this._outboxes.islander.dequeue(async())
            }, function (envelope) {
                if (envelope == null) {
                    return [ loop.break ]
                }
                async(function () {
                    var properties = this.paxos.government.properties[this.paxos.government.majority[0]]
                    this._requester.request('kibitz', {
                        module: 'kibitz',
                        method: 'enqueue',
                        to: properties,
                        body: {
                            islandId: this.paxos.islandId,
                            entries: envelope.messages
                        }
                    }, async())
                }, function (promises) {
                    this._islander.receipts(promises)
                })
            })
        })()
    })
})

// TODO Annoying how difficult it is to stop this crazy thing. There are going
// to be race conditions where we have a termination, come in, we shut things
// down, but then we continue with processing a pulse which triggers a timer.
// Sending messages to legislator can restart it's scheduler.
//
// TODO We could kill the timer in the scheduler, set the boolean we added to
// tell it to no longer schedule.
Kibitzer.prototype._send = cadence(function (async) {
    async([function () {
        this._pumping.unlock()
    }], function () {
        var loop = async(function () {
            async(function () {
                this._outboxes.legislator.dequeue(async())
            }, function (pulse) {
                if (pulse == null) {
                    return [ loop.break ]
                }
                var responses = []
                async(function () {
                    pulse.route.forEach(function (id) {
                        if (id == this.paxos.id) {
                            responses[id] = this.paxos.receive(this._Date.now(), pulse, pulse.messages)
                        } else {
                            async(function () {
                                var properties = this.paxos.government.properties[id]
                                this._requester.request('kibitz', {
                                    module: 'kibitz',
                                    method: 'receive',
                                    to: properties,
                                    body: pulse
                                }, async())
                            }, function (response) {
                                responses[id] = response
                            })
                        }
                    }, this)
                }, function () {
                    this.paxos.sent(this._Date.now(), pulse, responses)
                })
            })
        })()
    })
})

Kibitzer.prototype._immigrate = cadence(function (async, post) {
    assert(post.hops != null)
    var outcome = this.paxos.immigrate(this._Date.now(), post.islandId, post.id, post.cookie, post.properties)
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

Kibitzer.prototype._enqueue = cadence(function (async, post) {
    var promises = {}
    for (var i = 0, I = post.entries.length; i < I; i++) {
        var entry = post.entries[i]
        var outcome = this.paxos.enqueue(this._Date.now(), post.islandId, entry)
        if (!outcome.enqueued) {
            entries = null
            break
        }
        promises[entry.cookie] = outcome.promise
    }
    return [ promises ]
})

Kibitzer.prototype._receive = cadence(function (async, pulse) {
    return [ this.paxos.receive(this._Date.now(), pulse, pulse.messages) ]
})

// TODO Where is this being used? Why not just reference the Paxos properties
// directly?
Kibitzer.prototype.getProperties = function () {
    var properties = []
    for (var key in this.legislator.properties) {
        properties.push(this.legislator.properties[key])
    }
    return properties
}

module.exports = Kibitzer
