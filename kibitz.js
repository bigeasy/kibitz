var UserAgent = require('inlet/http/ua')
var cadence = require('cadence')
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
    this.discover = middleware.handle(this.discover.bind(this))
    this.receive = middleware.handle(this.receive.bind(this))
    this.enqueue = middleware.handle(this.enqueue.bind(this))
    this.sync = middleware.handle(this.sync.bind(this))
    this.participants = {}
    this.legislator = new Legislator('0')
    this.cookies = {}
    this.logger = options.logger || function () { console.log(arguments) }

    this.subscriber = turnstile(function () {
        var outbox = this.client.outbox()
        return outbox.length ? outbox : null
    }.bind(this), cadence([function (async, value) {
        async(function () {
            this.ua.fetch({
                url: this.leader()
            }, {
                url: '/enqueue',
                payload: {
                    entries: value
                }
            }, async())
        }, function (body, response) {
            if (!response.okay || !body.posted) {
                this.client.published(null)
                setTimeout(async(), 250)
            } else {
                this.client.published(body.entries)
            }
        })
    }, this.catcher('subscriber')
    ]).bind(this))

    this.publisher = turnstile(function () {
        var outbox = this.legislator.outbox()
        return outbox.length ? outbox : null
    }.bind(this), cadence([function (async, outbox) {
        async(function () {
            async(function (route) {
                var forwards = this.legislator.forwards(route.path, 0)
                async(function () {
                    var serialized = {
                        route:route,
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
                    if (!response.okay) {
                        setTimeout(async(), 2500)
                    }
                    var returns = response.okay ? body.returns : []
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
            async(function (entry) {
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
        })
    }, this.catcher('consumer')
    ]).bind(this))
}

//Error.stackTraceLimit = Infinity
Kibitzer.prototype.discover = cadence(function (async) {
    var urls = []
    for (var key in this.legislator.location) {
        urls.push(this.legislator.location[key])
    }
    return { urls: urls }
})

Kibitzer.prototype.receive = cadence(function (async, request) {
    var work = request.body
    var route = work.route, index = work.index, expanded = serializer.expand(work.messages)
    async(function () {
        this.legislator.inbox(route, expanded)
        route = this.legislator.routeOf(route.path)
        if (index + 1 < route.path.length) {
            async(function () {
                var forwards = this.legislator.forwards(route.path, index)
                this.ua.fetch(function () {
                    url: this.addresses[route.path[index + 1]]
                }, {
                    url: '/receive',
                    messages: serializer.flatten(forwards)
                })
            }, function (body, response) {
                if (response.okay) {
                    this.legislator.inbox(body.returns)
                }
            })
        }
    }, function () {
        this.consumer.nudge()
        return { returns: this.legislator.returns(route.path, index) }
    })
})

Kibitzer.prototype.createIdentifier = function (location) {
    var hash = crypto.createHash('md5')
    hash.update(crypto.pseudoRandomBytes(1024))
    var preferred = this.preferred ? 'a' : '7'
    return hash.digest('hex') + preferred
}

Kibitzer.prototype.bootstrap = function (location) {
    var id = this.createIdentifier()
    this.legislator = new Legislator(id, {
        peferred: function () { return this.id[this.id.length - 1] == 'a' }
    })
    this.legislator.bootstrap()
    this.client = new Client(this.legislator.id)
    this.client.prime(this.legislator.prime('1/0'))
    this.legislator.location[id] = location
}

Kibitzer.prototype.send = function (message) {
    if (this.legislator.government.majority[0] == this.legislator.id) {
        throw new Error
    }
}

Kibitzer.prototype.join = cadence(function (async, url) {
    async(function () {
        this.ua.fetch({ url: url, timeout: 2500 }, async())
    }, function (body, response) {
        var urls = body.urls
        if (urls.length == 0) {
            throw new Error('no other participants')
        }
        var index = 0, dataset = 'log', previous = null
        var sync = async(function () {
            this.ua.fetch({
                url: body.urls[index]
            }, {
                url: '/sync',
                payload: {
                    dataset: dataset,
                    previous: previous
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
                }
                break
            case 'meta':
                this.legislator.location = body.location
                this.since = body.promise
                dataset = 'user'
                previous = null
                break
            case 'user':
                previous = body.previous
                body.entries.forEach(function (entry) {
                    this.play(entry)
                }, this)
                if (previous == null) {
                    return [ async, true ]
                }
                break
            }
        })()
    }, function () {
        this.legislator.immigrate(this.createIdentifier())
        this.client = new Client(this.legislator.id)
        this.legislator.initialize()
        this.client.prime(this.legislator.prime(this.since))
        assert(this.client.length, 'no entries in client')
        this.consumer.nudge()
        var cookie = this.publish({
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
        return this.legislator.extract('reverse', 24, body.previous)
    case 'meta':
        return {
            promise: this.legislator.greatestOf(this.legislator.id).uniform,
            location: this.legislator.location
        }
    case 'user':
        return this.brief(body.previous)
    }
})

Kibitzer.prototype.enqueue = cadence(function (async, request) {
    var response = { entries: [] }
    request.body.entries.forEach(function (entry) {
        var outcome = this.legislator.post(entry.cookie, entry.value, entry.internal)
        // todo: I expect the cookie to be in the outcome.
        if (outcome.posted) {
            response.posted = true
            response.entries.push({ cookie: entry.cookie, promise: outcome.promise })
        }
    }, this)
    this.consumer.nudge()
    this.publisher.nudge()
    return response
})

Kibitzer.prototype.leader = function () {
    var id = this.legislator.government.majority[0]
    return this.legislator.location[id]
}

Kibitzer.prototype.catcher = function (context) {
    return function (_, error) {
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

module.exports = Kibitzer
