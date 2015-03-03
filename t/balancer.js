var UserAgent = require('inlet/http/ua')
var middleware = require('inlet/http/middleware')
var cadence = require('cadence/redux')

function Balancer (binder) {
    this.binder = binder
    this._ua = new UserAgent
    this._index = 0
    this.servers = []
}

Balancer.prototype.dispatch = function () {
    return middleware.dispatch(this.binder, {
        'GET /discover': middleware.handle(this.discover.bind(this))
    })
}

Balancer.prototype.discover = cadence(function (async, request) {
    if (this.servers.length === 0) {
        request.raise(503)
    }
    var index = this._index++
    if (this._index >= this.servers.length) {
        this._index = 0
    }
    async(function () {
        this._ua.fetch(this.servers[index], {
            url: '/discover'
        }, async())
    }, function (body, response, buffer) {
        return middleware.send(response.statusCode, response.headers, buffer)
    })
})

module.exports = Balancer
