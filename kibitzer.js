var assert = require('assert')

var RBTree = require('bintrees').RBTree

var cadence = require('cadence')

var Reactor = require('reactor')

var Id = require('paxos/id')
var Legislator = require('paxos/legislator')
var Client = require('paxos/client')

var Monotonic = require('monotonic')

var Scheduler = require('happenstance')

var interrupt = require('interrupt').createInterrupter()

function Kibitzer (id, options) {
    assert(id != null, 'id is required')

    options.ping || (options.ping = 250)
    options.timeout || (options.timeout = 1000)
    this.poll = options.poll || 5000
    this.ping = options.ping
    this.timeout = options.timeout

    this.syncLength = options.syncLength || 250

    this._reactor = new Reactor({ object: this, method: '_tick' })

    this.happenstance = new Scheduler
    this._Date = options.Date || Date

    this.joining = []

    this.url = options.url
    this._ua = options.ua

    this.islandId = null
    this.baseId = id
    this.suffix = '0'

    this.legislator = this._createLegislator()
    this.client = new Client(this.legislator.id)

    this.cookies = {}
    this.logger = options.logger || function () {}

    this.discovery = options.discovery

    this.available = false

    this.waits = new RBTree(function (a, b) { return Id.compare(a.promise, b.promise) })
}

Kibitzer.prototype._createLegislator = function () {
    var suffix = this.suffix
    var words = Monotonic.parse(this.suffix)
    this.suffix = Monotonic.toString(Monotonic.increment(words))
    return new Legislator(this.baseId + suffix, {
        ping: this.ping,
        timeout: this.timeout
    })
}

Kibitzer.prototype._tick = cadence(function (async) {
    var dirty = false
    async(function () {
        var outgoing = this.client.outbox()
        if (outgoing.length) {
            var post
            async(function () {
                var url = this.legislator.location[this.legislator.government.majority[0]]
                this._ua.enqueue(url, post = {
                    entries: outgoing
                }, async())
            }, function (body) {
                var published = body ? body.entries : []
                this.logger('info', 'enqueued', {
                    kibitzerId: this.legislator.id,
                    sent: post,
                    received: body
                })
                this.client.published(published)
            })
            dirty = true
        }
    }, function () {
        async.forEach(function (route) {
            var forwards = this.legislator.forwards(route, 0), serialized
            async(function () {
                serialized = {
                    route: route,
                    index: 1,
                    messages: serializer.flatten(forwards)
                }
                var url = this.legislator.location[route.path[1]]
                this._ua.receive(url, serialized, async())
            }, function (body) {
                var returns = body ? body.returns : []
                this.logger('info', 'published', {
                    kibitzerId: this.legislator.id,
                    sent: serialized,
                    received: body
                })
                this.legislator.inbox(route, returns)
                this.legislator.sent(route, forwards, returns)
            })
            dirty = true
        })(this.legislator.outbox())
    }, function () {
        var entries = this.legislator.since(this.client.uniform)
        if (entries.length) {
            this.logger('info', 'consuming', {
                kibitzerId: this.legislator.id,
                entries: entries
            })
            var promise = this.client.receive(entries)
            async.forEach(function (entry) {
                this.logger('info', 'consume', {
                    kibitzerId: this.legislator.id,
                    entry: entry
                })
                var callback = this.cookies[entry.cookie]
                if (callback) {
                    callback(null, entry)
                    delete this.cookies[entry.cookie]
                }
                var wait
                while ((wait = this.waits.min()) && Id.compare(wait.promise, entry.promise) <= 0) {
                    wait.callbacks.forEach(function (callback) { callback() })
                    this.waits.remove(wait)
                }
            })(this.client.since(promise))
            dirty = true
        }
    }, function () {
        if (dirty) {
            this._reactor.check()
        }
    })
})


Kibitzer.prototype._checkSchedule2 = cadence(function (async) {
    async.forEach(function (event) {
        var method = 'when' + event[0].toUpperCase() + event.substring(1)
        this[method](event, async())
    })(this.happenstance.check())
})

Kibitzer.prototype._schedule = function (type, delay) {
    this.happenstance.schedule(this.legislator.id, type, this._Date.now() + delay)
}

Kibitzer.prototype._checkSchedule = function () {
    this._interval = setInterval(function () {
        if (this.legislator.checkSchedule()) {
            this.publisher.nudge()
        }
        this.scheduler.nudge()
    }.bind(this), 50)
}

Kibitzer.prototype._urls = function () {
    var urls = []
    for (var key in this.legislator.location) {
        urls.push(this.legislator.location[key])
    }
    return urls
}

Kibitzer.prototype.discover = cadence(function (async) {
    var urls = this._urls()
    if (urls.length) {
        return { id: this.legislator.id, urls: urls }
    } else {
        return null
    }
})

Kibitzer.prototype.pull = cadence(function (async, url) {
    assert(url, 'url is missing')
    var dataset = 'log', post, next = null
    var sync = async(function () {
        this._ua.sync(url, post = {
            kibitzerId: this.legislator.id,
            dataset: dataset,
            next: next
        }, async())
    }, function (body) {
        this.logger('info', 'pulled', {
            kibitzerId: this.legislator.id,
            url: url,
            sent: post,
            received: body
        })
        if (!body) {
            throw this._unexceptional(new Error('unable to sync'))
        } else {
            this._schedule('joining', this.timeout)
            switch (dataset) {
            case 'log':
                this.legislator.inject(body.entries)
                if (body.next == null) {
                    // todo: fast forward client!
                    dataset = 'meta'
                    next = null
                }
                next = body.next
                break
            case 'meta':
                this.legislator.location = body.location
                this.since = body.promise
                return [ sync.break ]
            }
        }
    })()
})

Kibitzer.prototype._sync = cadence(function (async, post) {
    if (!this.available) {
        this.logger('info', 'sync', {
            kibitzerId: this.legislator.id,
            available: this.available,
            received: request.body
        })
        interrupt.panic(new Error, 'unsyncable')
    }
    var response
    switch (post.dataset) {
    case 'log':
         response = this.legislator.extract('reverse', 24, post.next)
         break
    case 'meta':
        response = {
            // todo: oh, so this is public
            promise: this.legislator._greatestOf(this.legislator.id).uniform,
            location: this.legislator.location
        }
        break
    }
    this.logger('info', 'sync', {
        kibitzerId: this.legislator.id,
        available: this.available,
        post: post,
        response: response
    })
    return response
})

Kibitzer.prototype.join = cadence(function (async, url) {
    if (this._urls().length) {
        return []
    }
    this.joining.push(async())
    this._schedule('join', 0)
})

Kibitzer.prototype._joined = function () {
    this.joining.splice(0, this.joining.length).forEach(function (callback) { callback() })
}

Kibitzer.prototype._naturalize = cadence(function (async, body) {
    this.logger('info', 'naturalize', {
        kibitzerId: this.legislator.id,
        received: body
    })
    async(function () {
        this.pull(body.urls[0], async())
    }, function () {
        this.bootstrapped = false
        this.legislator.immigrate(this.legislator.id)
        this.legislator.initialize()
        this.client.prime(this.legislator.prime(this.since))
        assert(this.client.length, 'no entries in client')
        this._reactor.turnstile.workers = 1
        this._reactor.check()
        this._schedule('joining', this.timeout)
        this.available = true
        this.publish({
            type: 'naturalize',
            id: this.legislator.id,
            location: this.url
        }, true, async())
    }, function () {
        this._joined()
    })
})

Kibitzer.prototype._rejoin = cadence(function (async, body) {
    async(function () {
        this.available = false
        this.client.clear().forEach(function (request) {
            var callback = this.cookies[request.cookie]
            assert(callback, 'request missing callback')
            delete this.cookies[request.cookie]
            callback(this._unexceptional(new Error('rejoining')))
        }, this)
        this.scram(async())
    }, function () {
        if (body) this._naturalize(body, async())
        else this._schedule('join', 0)
    })
})

Kibitzer.prototype.bootstrap = function (async) {
    this.bootstrapped = true
    this._reactor.turnstile.workers = 1
    this.legislator.bootstrap()
    this.logger('info', 'bootstrap', {
        kibitzerId: this.legislator.id
    })
    this.client.prime(this.legislator.prime('1/0'))
    this.legislator.location[this.legislator.id] = this.url
    this.available = true
}

Kibitzer.prototype.whenJoin = cadence(function (async) {
    async(function () {
        this._ua.discover(async())
    }, function (body, okay) {
        this.logger('info', 'join', {
            okay: okay,
            kibitzerId: this.legislator.id,
            received: JSON.stringify(body)
        })
        if (okay) {
            this._naturalize(body, async())
        } else {
            this._schedule('join', this.timeout)
        }
    })
})

Kibitzer.prototype.whenJoining = cadence(function (async) {
    this.logger('info', 'joining', {
        kibitzerId: this.legislator.id,
    })
    this._rejoin(async())
})

Kibitzer.prototype.publish = cadence(function (async, entry, internal) {
    var cookie = this.client.publish(entry, internal)
    this.cookies[cookie] = async()
    this._reactor.check()
})

Kibitzer.prototype._enqueue = cadence(function (async, post) {
    if (!this.available) {
        this.logger('info', 'enqueue', {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            available: this.available,
            received: request.body
        })
        request.raise(517)
    }
    var response = { islandId: this.islandId, posted: false, entries: [] }
    post.entries.forEach(function (entry) {
        var outcome = this.legislator.post(entry.cookie, entry.value, entry.internal)
        // todo: I expect the cookie to be in the outcome.
        // todo: test receiving entries, enqueuing, when we are not the leader.
        if (outcome.posted) {
            response.posted = true
            response.entries.push({ cookie: entry.cookie, promise: outcome.promise })
        }
    }, this)
    this._reactor.check()
    this.logger('info', 'enqueue', {
        kibitzerId: this.legislator.id,
        islandId: this.islandId,
        available: this.available,
        received: post,
        response: response
    })
    return response
})

Kibitzer.prototype._receive = cadence(function (async, post) {
    if (!this.available) {
        this.logger('info', 'receive', {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            available: this.available,
            received: request.body
        })
        return null
        // request.raise(517)
    }
    var route = post.route, index = post.index, expanded = serializer.expand(post.messages)
    async(function () {
        route = this.legislator.routeOf(route.path, route.pulse)
        this.legislator.inbox(route, expanded)
        if (index + 1 < route.path.length) {
            async(function () {
                var forwards = this.legislator.forwards(route, index)
                var serialized = {
                    islandId: this.islandId,
                    route: route,
                    index: index + 1,
                    messages: serializer.flatten(forwards)
                }
                this.ua.fetch(
                    this.createBinder(this.legislator.location[route.path[index + 1]])
                , {
                    url: '/receive',
                    payload: serialized
                }, async())
            }, function (body, response) {
                var returns = this._response(response, body, 'returns', 'returns', [])
                this.legislator.inbox(route, returns)
            })
        }
    }, function () {
        var returns = this.legislator.returns(route, index)
        this._reactor.check()
        this.logger('info', 'receive', {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            available: this.available,
            received: post,
            returns: returns
        })
        return { returns: returns }
    })
})

Kibitzer.prototype.wait = function (promise, callback) {
    if (Id.compare(promise, this.client.uniform) <= 0) {
        callback()
    } else {
        var wait = this.waits.find({ promise: promise })
        if (!wait) {
            wait = { promise: promise, callbacks: [] }
            this.waits.insert(wait)
        }
        wait.callbacks.push(callback)
    }
}

Kibitzer.prototype.stop = cadence(function (async) {
    this.scheduler.workers = 0
    if (this._interval != null) {
        clearInterval(this._interval)
        this._interval = null
    }
    this.scram(async())
})

module.exports = Kibitzer
