var UserAgent = require('inlet/http/ua')
var cadence = require('cadence/redux')
require('cadence/loops')
var Legislator = require('paxos/legislator')
var Client = require('paxos/client')
var Scheduler = require('happenstance')
var Binder = require('inlet/net/binder')
var middleware = require('inlet/http/middleware')
var crypto = require('crypto')
var assert = require('assert')
var turnstile = require('turnstile')
var serializer = require('paxos/serializer')
var Id = require('paxos/id')
var RBTree = require('bintrees').RBTree
var Monotonic = require('monotonic')

function Ignore () {
}

Ignore.prototype.play = function (callback) {
    callback()
}

function Kibitzer (id, options) {
    assert(id != null, 'id is required')
    id = (options.preferred ? 'a' : '7') + id

    options.ping || (options.ping = [ 250, 250 ])
    options.timeout || (options.timeout = [ 1000, 1000 ])
    this.poll = options.poll || [ 5000, 7000 ]

    this.suffix = '0'
    this.ping = options.ping
    this.timeout = (typeof options.timeout == 'number')
                 ? [ options.timeout, options.timeout ] : options.timeout

    this.syncLength = options.syncLength || 250

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
    this.baseId = id
    this.legislator = this._createLegislator()
    this.client = new Client(this.legislator.id)
    this._createTurnstiles(this.instance = {
        islandId: null,
        legislator: this.legislator,
        client: this.client,
        player: this.player
    })
    this.cookies = {}
    this.logger = options.logger || function (level, message, context) {
        return
        var error
        if (error = context.error) {
            delete context.error
            context.message = error.message
        }
        console.log(level, message, JSON.stringify(context))
        if (error && !error.unexceptional) {
            console.log(error.stack)
        }
    }
    this.createBinder = options.createBinder || function (url) { return new Binder(url) }
    this.discovery = options.discovery

    this.available = false

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
    this.scheduler.workers = Infinity
}

Kibitzer.prototype._unexceptional = function (error) {
    error.unexceptional = true
    return error
}

Kibitzer.prototype.scram = cadence(function (async) {
    async(function () {
        this.legislator = this._createLegislator()
        this.client = new Client(this.legislator.id)
        this._createTurnstiles(this.instance = {
            islandId: null,
            legislator: this.legislator,
            client: this.client,
            player: this.player
        })
        this.islandId = null
        this.instance.player = new Ignore()
        this.consumer.workers = 0
        this.publisher.workers = 0
        this.subscriber.workers = 0
    }, function () {
        this.player.scram(async())
    })
})

Kibitzer.prototype._createLegislator = function () {
    var suffix = this.suffix
    var words = Monotonic.parse(this.suffix)
    this.suffix = Monotonic.toString(Monotonic.increment(words))
    return new Legislator(this.baseId + suffix, {
        prefer: function (id) { return id[0] === 'a' },
        ping: this.ping,
        timeout: this.timeout
    })
}

Kibitzer.prototype._createTurnstiles = function (instance) {
    this.subscriber = turnstile(function () {
        var outbox = instance.client.outbox()
        return outbox.length ? outbox : null
    }.bind(this), cadence([function (async, value) {
        var payload
        async(function () {
            this.ua.fetch(
                this.createBinder(instance.legislator.location[instance.legislator.government.majority[0]])
            , {
                url: '/enqueue',
                payload: payload = {
                    islandId: instance.islandId,
                    entries: value
                }
            }, async())
        }, function (body, response) {
            var published = this._response(response, body, 'posted', 'entries', [])
            this.logger('info', 'enqueued', {
                kibitzerId: instance.legislator.id,
                islandId: instance.islandId,
                statusCode: response.statusCode,
                sent: payload,
                received: body
            })
            instance.client.published(published)
        })
    }, this.catcher('subscriber')
    ]).bind(this))

    this.publisher = turnstile(function () {
        var outbox = instance.legislator.outbox()
        return outbox.length ? outbox : null
    }.bind(this), cadence([function (async, outbox) {
        async(function () {
            async.forEach(function (route) {
                var forwards = instance.legislator.forwards(route, 0), serialized
                async(function () {
                    serialized = {
                        islandId: instance.islandId,
                        route: route,
                        index: 1,
                        messages: serializer.flatten(forwards)
                    }
                    this.ua.fetch(
                        this.createBinder(instance.legislator.location[route.path[1]])
                    , {
                        url: '/receive',
                        payload: serialized
                    }, async())
                }, function (body, response) {
                    var returns = this._response(response, body, 'returns', 'returns', [])
                    this.logger('info', 'published', {
                        kibitzerId: instance.legislator.id,
                        islandId: instance.islandId,
                        statusCode: response.statusCode,
                        sent: serialized,
                        received: body
                    })
                    instance.legislator.inbox(route, returns)
                    instance.legislator.sent(route, forwards, returns)
                })
            })(outbox)
        }, function () {
            this.consumer.nudge()
        })
    }, this.catcher('publisher')
    ]).bind(this))

    this.consumer = turnstile(function () {
        var entries = instance.legislator.since(instance.client.uniform)
        return entries.length ? entries : null
    }.bind(this), cadence([function (async, entries) {
        async(function () {
            setImmediate(async())
        }, function () {
            this.logger('info', 'consuming', {
                kibitzerId: instance.legislator.id,
                islandId: instance.islandId,
                entries: entries
            })
            var promise = instance.client.receive(entries)
            async.forEach(function (entry) {
                this.logger('info', 'consume', {
                    kibitzerId: instance.legislator.id,
                    islandId: instance.islandId,
                    entry: entry
                })
                var callback = this.cookies[entry.cookie]
                if (callback) {
                    callback(null, entry)
                    delete this.cookies[entry.cookie]
                }
                async([function () {
                    instance.player.play(entry, async())
                }, this.catcher('play')
                ], function () {
                    var wait
                    while ((wait = this.waits.min()) && Id.compare(wait.promise, entry.promise) <= 0) {
                        wait.callbacks.forEach(function (callback) { callback() })
                        this.waits.remove(wait)
                    }
                })
            })(instance.client.since(promise))
        }, function () {
            this.subscriber.nudge()
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
    this.scheduler.workers = Infinity
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
    if (!this.available || this.islandId != request.body.islandId)  {
        this.logger('info', 'receive', {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            available: this.available,
            received: request.body
        })
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
        this.consumer.nudge()
        this.logger('info', 'receive', {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            available: this.available,
            received: request.body,
            returns: returns
        })
        return { returns: returns }
    })
})

Kibitzer.prototype._checkPullIsland = function (islandId) {
    if (this.islandId != islandId) {
        throw new Error('island change')
    }
}

Kibitzer.prototype.pull = cadence(function (async, url) {
    assert(url, 'url is missing')
    var dataset = 'log', payload, next = null, islandId = this.islandId
    var sync = async(function () {
        this.ua.fetch(
            this.createBinder(url)
        , {
            url: '/sync',
            payload: payload = {
                kibitzerId: this.legislator.id,
                islandId: this.islandId,
                dataset: dataset,
                next: next
            }
        }, async())
    }, function (body, response) {
        this._checkPullIsland(islandId)
        this.logger('info', 'pulled', {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            url: url,
            statusCode: response.statusCode,
            sent: payload,
            received: body
        })
        if (!response.okay) {
            throw this._unexceptional(new Error('unable to sync'))
        } else {
            this._schedule('joining', this.timeout[0])
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
                dataset = 'user'
                break
            case 'user':
                async(function () {
                    async.forEach(function (entry) {
                        this.player.play(entry, async())
                    }, function () {
                        this._checkPullIsland(islandId)
                        this._schedule('joining', this.timeout[0])
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
        this.available = false
        this.client.clear().forEach(function (request) {
            var callback = this.cookies[request.cookie]
            if (callback) {
                delete this.cookies[request.cookie]
                callback(this._unexceptional(new Error('rejoining')))
            }
        }, this)
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
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            statusCode: response.statusCode,
            received: body
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
    this.islandId = this.instance.islandId = body.islandId
    this.logger('info', 'naturalize', {
        kibitzerId: this.legislator.id,
        islandId: this.islandId,
        preferred: this.preferred,
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
        this.consumer.workers = 1
        this.publisher.workers = 1
        this.subscriber.workers = 1
        this.consumer.nudge()
        this._schedule('joining', this.timeout[0])
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

Kibitzer.prototype.whenJoining = cadence(function (async) {
    this._rejoin(async())
})

Kibitzer.prototype.whenJoin = cadence(function (async) {
    async([function () {
        async(function () {
            this.ua.fetch(this.discovery, { timeout: this.timeout[0] }, async())
        }, function (body, response) {
            this.logger('info', 'join', {
                kibitzerId: this.legislator.id,
                islandId: this.islandId,
                statusCode: response.statusCode,
                received: body,
                preferred: this.preferred
            })
            if (response.okay && body.urls.length) {
                this._naturalize(body, async())
            } else if (!this.preferred) {
                this.logger('info', 'retry', {
                    kibitzerId: this.legislator.id,
                    islandId: this.islandId,
                    preferred: this.preferred
                })
                this._schedule('join', this.timeout)
            } else {
                this.bootstrapped = true
                this.islandId = this.instance.islandId = this.legislator.id // this.previousIslandId = Id.increment(this.previousIslandId, 1)
                this.consumer.workers = 1
                this.publisher.workers = 1
                this.subscriber.workers = 1
                this.legislator.bootstrap()
                this.logger('info', 'bootstrap', {
                    kibitzerId: this.legislator.id,
                    islandId: this.islandId,
                    preferred: this.preferred
                })
                this.client.prime(this.legislator.prime('1/0'))
                this.legislator.location[this.legislator.id] = this.url
                this.available = true
                this._joined()
            }
        })
    }, function (error) {
        this.logger(error.unexceptional ? 'info' : 'error', 'whenJoin', {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            error: error
        })
    }])
})

Kibitzer.prototype.publish = cadence(function (async, entry, internal) {
    var cookie = this.client.publish(entry, internal)
    this.cookies[cookie] = async()
    this.subscriber.nudge()
})

//Error.stackTraceLimit = Infinity
Kibitzer.prototype._sync = cadence(function (async, request) {
    var body = request.body
    if (!this.available || this.islandId !== request.body.islandId) {
        this.logger('info', 'sync', {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            available: this.available,
            received: request.body
        })
        request.raise(517)
    }
    var response
    switch (body.dataset) {
    case 'log':
         response = this.legislator.extract('reverse', 24, body.next)
         break
    case 'meta':
        response = {
            promise: this.legislator.greatestOf(this.legislator.id).uniform,
            location: this.legislator.location
        }
        break
    case 'user':
        response = this.player.brief(body.next)
        break
    }
    this.logger('info', 'sync', {
        kibitzerId: this.legislator.id,
        islandId: this.islandId,
        available: this.available,
        received: request.body,
        response: response
    })
    return response
})

Kibitzer.prototype._enqueue = cadence(function (async, request) {
    if (!this.available || this.islandId != request.body.islandId) {
        this.logger('info', 'enqueue', {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            available: this.available,
            received: request.body
        })
        request.raise(517)
    }
    var response = { islandId: this.islandId, posted: false, entries: [] }
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
    this.logger('info', 'enqueue', {
        kibitzerId: this.legislator.id,
        islandId: this.islandId,
        available: this.available,
        received: request.body,
        response: response
    })
    return response
})

Kibitzer.prototype.catcher = function (context) {
    return function (error) {
        this.logger(error.unexceptional ? 'info' : 'error', context, {
            kibitzerId: this.legislator.id,
            islandId: this.islandId,
            error: error
        })
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

Kibitzer.prototype.stop = cadence(function (async) {
    if (this._interval != null) {
        clearInterval(this._interval)
        this._interval = null
    }
    this.scram(async())
})

module.exports = Kibitzer
