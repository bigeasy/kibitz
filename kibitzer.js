var UserAgent = require('inlet/http/ua')
var cadence = require('cadence/redux')
require('cadence/loops')
var Legislator = require('paxos/legislator')
var Client = require('paxos/client')
var Scheduler = require('happenstance')
var middleware = require('inlet/http/middleware')
var crypto = require('crypto')
var assert = require('assert')
var turnstile = require('turnstile')
var serializer = require('paxos/serializer')
var Id = require('paxos/id')
var RBTree = require('bintrees').RBTree

function Kibitzer (id, options) {
    assert(id != null, 'id is required')
    id = (options.preferred ? 'a' : '7') + id

    options.ping || (options.ping = [ 250, 250 ])
    options.timeout || (options.timeout = [ 1000, 1000 ])
    this.poll = options.poll || [ 5000, 7000 ]

    this.ping = options.ping
    this.timeout = (typeof options.timeout == 'number')
                 ? [ options.timeout, options.timeout ] : options.timeout

    this.islandId = null
    this.previousIslandId = id + '/1'
    this.preferred = !! options.preferred
    this.happenstance = new Scheduler(options.clock || function () { return Date.now() })
    this.joining = []
    this.waits = new RBTree(function (a, b) { return Id.compare(a.promise, b.promise) })
    this.preferred = !!options.preferred
    this.ua = new UserAgent
    this.url = options.url
    this.player = options.player
    this.discover = middleware.handle(this._discover.bind(this))
    this.receive = middleware.handle(this._receive.bind(this))
    this.enqueue = middleware.handle(this._enqueue.bind(this))
    this.sync = middleware.handle(this._sync.bind(this))
    this.participants = {}
    this.legislator = this._createLegislator(id)
    this.client = new Client(this.legislator.id)
    this.cookies = {}
    this.logger = options.logger || function () { console.log(arguments) }
    this.discovery = options.discovery

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
                    islandId: this.islandId,
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
                        islandId: this.islandId,
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
                    this.player.play(entry, async())
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

    this.scheduler = turnstile(function () {
        var events = this.happenstance.check()
        return events.length ? events : null
    }.bind(this), cadence([function (async, events) {
        async.forEach(function (event) {
            var type = event.type
            var method = 'when' + type[0].toUpperCase() + type.substring(1)
            this[method](event, async())
        })(events)
    }, this.catcher('scheduler')
    ]).bind(this))
}

Kibitzer.prototype.scram = cadence(function (async) {
    async(function () {
        this.legislator = this._createLegislator(this.legislator.id)
        this.client = new Client(this.legislator.id)
        this.islandId = null
        this.consumer.workers = 0
        this.publisher.workers = 0
        this.subscriber.workers = 0
    }, function () {
        this.player.scram(async())
    })
})

Kibitzer.prototype._createLegislator = function (id) {
    return new Legislator(id, {
        prefer: function (id) { return id[0] === 'a' },
        ping: this.ping,
        timeout: this.timeout
    })
}

Kibitzer.prototype._response = function (response, body, condition, values, failure) {
    if (response.okay && body[condition]) {
        return body[values]
    }
    return failure
}

Kibitzer.prototype._urls = function () {
    var urls = []
    for (var key in this.legislator.location) {
        urls.push(this.legislator.location[key])
    }
    return urls
}

Kibitzer.prototype._discover = cadence(function (async, request) {
    var urls = this._urls()
    urls.sort(function (left, right) {
        var a = this.legislator.location[this.legislator.id] == left
        var b = this.legislator.location[this.legislator.id] == right
        return (a && b) || !(a || b) ? 0 : a ? -1 : 1
    }.bind(this))
    return { id: this.legislator.id, islandId: this.islandId, urls: urls }
})

Kibitzer.prototype._checkSchedule = function () {
    this._interval = setInterval(function () {
        if (this.legislator.checkSchedule()) {
            this.publisher.nudge()
        }
        this.scheduler.nudge()
    }.bind(this), 50)
}

Kibitzer.prototype._receive = cadence(function (async, request) {
    if (this.islandId != request.body.islandId) {
        request.raise(517)
    }
    var work = request.body
    var route = work.route, index = work.index, expanded = serializer.expand(work.messages)
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

Kibitzer.prototype.pull = cadence(function (async, urls) {
    var index = 0, dataset = 'log', next = null
    var sync = async(function () {
        this.ua.fetch({
            url: urls[index]
        }, {
            url: '/sync',
            payload: {
                sourceId: this.id,
                islandId: this.islandId,
                dataset: dataset,
                next: next
            }
        }, async())
    }, function (body, response) {
        if (!response.okay) {
            async(function () {
                this._rejoin(async())
            }, function () {
                return [ sync ]
            })
        } else {
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
                        this.player.play(entry, async())
                    })(body.entries)
                }, function () {
                    next = body.next
                    if (next == null) {
                        return [ sync ]
                    }
                })
                break
            }
        }
    })()
})

Kibitzer.prototype.join = cadence(function (async, url) {
    this.logger('info', 'join')
    if (this._urls().length) {
        return []
    }
    this.joining.push(async())
    if (!this._interval) {
        this._checkSchedule()
    }
    this._schedule('join', 0)
})

Kibitzer.prototype._schedule = function (type, delay) {
    this.happenstance.schedule({
        id: this.legislator.id,
        delay: delay,
        value: { type: type }
    })
}

Kibitzer.prototype._joined = function () {
    this._schedule(this.preferred ? 'preferred' : 'notPreferred', this.ping)
    this.joining.splice(0, this.joining.length).forEach(function (callback) { callback() })
}

Kibitzer.prototype._rejoin = cadence(function (async, body) {
    async(function () {
        this.scram(async())
    }, function () {
        if (body) this._naturalize(body, async())
        else this._schedule('join', 0)
    })
})

Kibitzer.prototype.whenNotPreferred = cadence(function (async) {
    this.shouldRejoin('notPreferred', function (body) {
        return body.islandId != this.islandId
    }.bind(this), async())
})

Kibitzer.prototype.whenPreferred = cadence(function (async) {
    this.shouldRejoin('preferred', function (body) {
        return body.islandId < this.islandId
    }.bind(this), async())
})

Kibitzer.prototype.shouldRejoin = cadence(function (async, type, condition) {
    async(function () {
        this.ua.fetch(this.discovery, { timeout: this.timeout[0] }, async())
    }, function (body, response) {
        this.logger('info', type + 'Rediscover', {
            statusCode: response.statusCode, payload: body
        })
        if (response.okay && body.urls.length && condition(body)) {
            this._rejoin(body, async())
        } else {
            this._schedule(type, this.timeout)
        }
    })
})

Kibitzer.prototype._naturalize = cadence(function (async, body) {
    assert(this.islandId == null, 'island id not reset')
    this.islandId = body.islandId
    async(function () {
        this.pull(body.urls, async())
    }, function () {
        this.bootstrapped = false
        this.legislator.immigrate(this.legislator.id)
        this.legislator.initialize()
        this.client.prime(this.legislator.prime(this.since))
        assert(this.client.length, 'no entries in client')
        this.consumer.workers = 1
        this.publisher.workers = 1
        this.subscriber.workers = 1
        this.consumer.nudge()
        this.publish({
            type: 'naturalize',
            id: this.legislator.id,
            location: this.url
        }, true, async())
    }, function () {
        this._joined()
    })
})

Kibitzer.prototype.whenJoin = cadence(function (async) {
    async([function () {
        async(function () {
            this.ua.fetch(this.discovery, { timeout: this.timeout[0] }, async())
        }, function (body, response) {
            if (response.okay && body.urls.length) {
                this._naturalize(body, async())
            } else if (!this.preferred) {
                this._schedule('join', this.timeout)
            } else {
                this.bootstrapped = true
                this.islandId = this.legislator.id // this.previousIslandId = Id.increment(this.previousIslandId, 1)
                this.consumer.workers = 1
                this.publisher.workers = 1
                this.subscriber.workers = 1
                this.legislator.bootstrap()
                this.client.prime(this.legislator.prime('1/0'))
                this.legislator.location[this.legislator.id] = this.url
                this._joined()
            }
        })
    }, this._catchJoinError])
})

Kibitzer.prototype._catchJoinError = function (error) {
    this.joining.slice(0, this.joining.length).forEach(function (callback) {
        callback(error)
    })
}

Kibitzer.prototype.publish = cadence(function (async, entry, internal) {
    var cookie = this.client.publish(entry, internal)
    this.cookies[cookie] = async()
    this.subscriber.nudge()
})

//Error.stackTraceLimit = Infinity
Kibitzer.prototype._sync = cadence(function (async, request) {
    var body = request.body
    if (this.islandId !== request.body.islandId) {
        request.raise(517)
    }
    switch (body.dataset) {
    case 'log':
        return this.legislator.extract('reverse', 24, body.next)
    case 'meta':
        return {
            promise: this.legislator.greatestOf(this.legislator.id).uniform,
            location: this.legislator.location
        }
    case 'user':
        return this.player.brief(body.next)
    }
})

Kibitzer.prototype._enqueue = cadence(function (async, request) {
    if (this.islandId != request.body.islandId) {
        request.raise(517)
    }
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
