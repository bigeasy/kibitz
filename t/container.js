var middleware = require('inlet/http/middleware')
var Kibitzer = require('..')
var RBTree = require('bintrees').RBTree
var cadence = require('cadence/redux')

function Container (binder, id, options) {
    this.lookup = new RBTree(function (a, b) { return a.key - b.key })
    this.binder = binder
    options.player = this
    options.url = binder.location
    this.kibitzer = new Kibitzer(id, options)
}

Container.prototype.dispatch = function () {
    return middleware.dispatch(this.binder, {
        'GET /discover': this.kibitzer.discover,
        'POST /enqueue': this.kibitzer.enqueue,
        'POST /receive': this.kibitzer.receive,
        'POST /sync': this.kibitzer.sync
    })
}

Container.prototype.play = cadence(function (async, entry) {
    if (entry.internal) {
        return
    }
    switch (entry.value.type) {
    case 'add':
        this.lookup.insert({ key: entry.value.key, value: entry.value.value })
        break
    case 'remove':
        this.lookup.remove({ key: entry.value.key })
        break
    case 'abend':
        throw new Error('abended')
    }
})

Container.prototype.brief = function (next) {
    var sought = next ? { key: next } : this.lookup.max()
    if (sought == null) {
        return { next: null, entries: [] }
    }
    var iterator = this.lookup.findIter(sought), entries = [], entry, count = 24
    while ((entry = iterator.data()) && count--) {
        entries.push({ type: 'add', value: entry })
        iterator.prev()
    }
    return { next: entry && entry.key, entries: entries }
}

Container.prototype.scram = function (callback) {
    this.lookup = new RBTree(function (a, b) { return a.key - b.key })
    callback()
}

module.exports = Container
