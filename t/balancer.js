var UserAgent = require('vizsla')
var Dispatcher = require('inlet/dispatcher')
var cadence = require('cadence')

function Balancer () {
    this._ua = new UserAgent
    this._index = 0
    this.servers = []
}

Balancer.prototype.dispatcher = function (options) {
    var dispatcher = new Dispatcher(this)
    dispatcher.dispatch('GET /discover', 'discover')
    return dispatcher.createDispatcher()
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
        return Dispatcher.resend(response.statusCode, response.headers, buffer)
    })
})

module.exports = Balancer
