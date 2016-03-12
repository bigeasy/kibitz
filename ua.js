var Vizsla = require('vizsla')

function UserAgent (discoveryUrl) {
    this._ua = new Vizsla
    this._discoveryUrl = discoveryUrl
}

UserAgent.prototype.discover = function (callback) {
    this._ua.fetch(this._session, { url: this._discoveryUrl, raise: true }, callback)
}

UserAgent.prototype.sync = function (location, post, callback) {
    this._ua.fetch({
        url: location
    }, {
        url: 'sync',
        post: post,
        raise: true
    }, callback)
}

UserAgent.prototype.enqueue = function (location, post, callback) {
    this._ua.fetch({
        url: location
    }, {
        url: 'enqueue',
        post: post,
        raise: true
    }, callback)
}

UserAgent.prototype.receive = function (location, post, callback) {
    this._ua.fetch({
        url: location
    }, {
        url: 'receive',
        post: post,
        raise: true
    }, callback)
}

module.exports = UserAgent
