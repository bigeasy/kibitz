var UserAgent = require('inlet/http/ua')
var cadence = require('cadence/redux')
require('cadence/loops')
var Legislator = require('paxos/legislator')
var Client = require('paxos/client')
var middleware = require('inlet/http/middleware')
var crypto = require('crypto')
var assert = require('assert')
var turnstile = require('turnstile')
var serializer = require('paxos/serializer')
var Id = require('paxos/id')
var RBTree = require('bintrees').RBTree

function Kibitzer (options) {
    this.waits = new RBTree(function (a, b) { return Id.compare(a.promise, b.promise) })
    this.preferred = !!options.preferred
    this.ua = new UserAgent
    this.brief = options.brief
    this.url = options.url
    this.play = options.play
    this.discover = middleware.handle(this._discover.bind(this))
    this.receive = middleware.handle(this._receive.bind(this))
    this.enqueue = middleware.handle(this._enqueue.bind(this))
    this.sync = middleware.handle(this.sync.bind(this))
    this.participants = {}
    this.legislator = new Legislator(this.createIdentifier(), {
        prefer: function (id) { return id[0] === 'a' },
        ping: [ 250, 250 ],
        timeout: [ 2000, 2000 ]
    })
    this.cookies = {}
    this.logger = options.logger || function () { console.log(arguments) }

    this.subscriber = turnstile(function () {
        var outbox = this.client.outbox()
        return outbox.length ? outbox : null
    }.bind(this), cadence([function (async, value) {
        async(function () {
            this.ua.fetch({
                url: this.legislator.location[this.legislator.government.majority[0]]
            }, {
                url: '/enqueue',
                payload: {
                    entries: value
                }
            }, async())
        }, function (body, response) {
            var published = this._response(response, body, 'posted', 'entries', null)
            this.client.published(published)
        })
    }, this.catcher('subscriber')
    ]).bind(this))

    this.publisher = turnstile(function () {
        var outbox = this.legislator.outbox()
        return outbox.length ? outbox : null
    }.bind(this), cadence([function (async, outbox) {
        async(function () {
            async.forEach(function (route) {
                var forwards = this.legislator.forwards(route, 0)
                async(function () {
                    var serialized = {
                        route: route,
                        index: 1,
                        messages: serializer.flatten(forwards)
                    }
                    this.ua.fetch({
                        url: this.legislator.location[route.path[1]]
                    }, {
                        url: '/receive',
                        payload: serialized
                    }, async())
                }, function (body, response) {
                    var returns = this._response(response, body, 'returns', 'returns', [])
                    this.legislator.inbox(route, returns)
                    this.legislator.sent(route, forwards, returns)
                })
            })(outbox)
        }, function () {
            this.consumer.nudge()
        })
    }, this.catcher('publisher')
    ]).bind(this))

    this.consumer = turnstile(function () {
        var entries = this.legislator.since(this.client.uniform)
        return entries.length ? entries : null
    }.bind(this), cadence([function (async, entries) {
        async(function () {
            setImmediate(async())
        }, function () {
            var promise = this.client.receive(entries)
            async.forEach(function (entry) {
                var callback = this.cookies[entry.cookie]
                if (callback) {
                    callback(null, entry)
                    delete this.cookies[entry.cookie]
                }
                async([function () {
                    this.play(entry, async())
                }, this.catcher('play')
                ], function () {
                    var wait
                    while ((wait = this.waits.min()) && Id.compare(wait.promise, entry.promise) <= 0) {
                        wait.callbacks.forEach(function (callback) { callback() })
                        this.waits.remove(wait)
                    }
                })
            })(this.client.since(promise))
        }, function () {
            this.publisher.nudge()
        })
    }, this.catcher('consumer')
    ]).bind(this))
}

Kibitzer.prototype._response = function (response, body, condition, values, failure) {
    if (response.okay && body[condition]) {
        return body[values]
    }
    return failure
}

//Error.stackTraceLimit = Infinity
Kibitzer.prototype._discover = cadence(function (async) {
    var urls = []
    for (var key in this.legislator.location) {
        urls.push(this.legislator.location[key])
    }
    return { urls: urls }
})

Kibitzer.prototype._checkSchedule = function () {
    this._interval = setInterval(function () {
        if (this.legislator.checkSchedule()) {
            this.publisher.nudge()
        }
    }.bind(this), 50)
}

Kibitzer.prototype._receive = cadence(function (async, request) {
    var work = request.body
    var route = work.route, index = work.index, expanded = serializer.expand(work.messages)
    async(function () {
        route = this.legislator.routeOf(route.path, route.pulse)
        this.legislator.inbox(route, expanded)
        if (index + 1 < route.path.length) {
            async(function () {
                var forwards = this.legislator.forwards(route, index)
                var serialized = {
                    route: route,
                    index: index + 1,
                    messages: serializer.flatten(forwards)
                }
                this.ua.fetch({
                    url: this.legislator.location[route.path[index + 1]]
                }, {
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
        this.consumer.nudge()
        return { returns: returns }
    })
})

Kibitzer.prototype.createIdentifier = function () {
    var hash = crypto.createHash('md5')
    hash.update(crypto.pseudoRandomBytes(1024))
    var preferred = this.preferred ? 'a' : '7'
    return preferred + hash.digest('hex')
}

Kibitzer.prototype.bootstrap = function (location) {
    this.legislator.bootstrap()
    this.client = new Client(this.legislator.id)
    this.client.prime(this.legislator.prime('1/0'))
    this.legislator.location[this.legislator.id] = location
    this._checkSchedule()
}

Kibitzer.prototype.pull = cadence(function (async, urls) {
    if (urls.length == 0) {
        throw new Error('no other participants')
    }
    var index = 0, dataset = 'log', next = null
    var sync = async(function () {
        this.ua.fetch({
            url: urls[index]
        }, {
            url: '/sync',
            payload: {
                dataset: dataset,
                next: next
            }
        }, async())
    }, function (body, response) {
        if (!response.okay) {
            if (++index == urls.length) {
                throw new Error('cannot find a participant to sync with')
            } else {
                return [ sync() ]
            }
        }
        switch (dataset) {
        case 'log':
            this.legislator.inject(body.entries)
            if (body.next == null) {
                dataset = 'meta'
                next = null
            }
            next = body.next
            break
        case 'meta':
            this.legislator.location = body.location
            this.since = body.promise
            dataset = 'user'
            break
        case 'user':
            async(function () {
                async.forEach(function (entry) {
                    this.play(entry, async())
                })(body.entries)
            }, function () {
                next = body.next
                if (next == null) {
                    return [ sync ]
                }
            })
            break
        }
    })()
})

Kibitzer.prototype.join = cadence(function (async, url) {
    async(function () {
        this.ua.fetch({ url: url, timeout: 2500 }, async())
    }, function (body, response) {
        this.pull(body.urls, async())
    }, function () {
        this.legislator.immigrate(this.legislator.id)
        this._checkSchedule()
        this.client = new Client(this.legislator.id)
        this.legislator.initialize()
        this.client.prime(this.legislator.prime(this.since))
        assert(this.client.length, 'no entries in client')
        this.consumer.nudge()
        this.publish({
            type: 'naturalize',
            id: this.legislator.id,
            location: this.url
        }, true, async())
    })
})

Kibitzer.prototype.publish = cadence(function (async, entry, internal) {
    var cookie = this.client.publish(entry, internal)
    this.cookies[cookie] = async()
    this.subscriber.nudge()
})

Kibitzer.prototype.sync = cadence(function (async, request) {
    var body = request.body
    switch (body.dataset) {
    case 'log':
        return this.legislator.extract('reverse', 24, body.next)
    case 'meta':
        return {
            promise: this.legislator.greatestOf(this.legislator.id).uniform,
            location: this.legislator.location
        }
    case 'user':
        return this.brief(body.next)
    }
})

Kibitzer.prototype._enqueue = cadence(function (async, request) {
    var response = { posted: false, entries: [] }
    request.body.entries.forEach(function (entry) {
        var outcome = this.legislator.post(entry.cookie, entry.value, entry.internal)
        // todo: I expect the cookie to be in the outcome.
        // todo: test receiving entries, enqueuing, when we are not the leader.
        if (outcome.posted) {
            response.posted = true
            response.entries.push({ cookie: entry.cookie, promise: outcome.promise })
        }
    }, this)
    this.consumer.nudge()
    this.publisher.nudge()
    return response
})

Kibitzer.prototype.catcher = function (context) {
    return function (error) {
        this.logger('error', context, error)
    }.bind(this)
}

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

Kibitzer.prototype.stop = function () {
    if (this._interval != null) {
        clearInterval(this._interval)
        this._interval = null
    }
}

module.exports = Kibitzer
