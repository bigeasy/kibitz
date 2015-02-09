var middleware = require('inlet/http/middleware')
var Kibitzer = require('..')
var RBTree = require('bintrees').RBTree
var cadence = require('cadence/redux')

function Container (binder, options) {
    this.lookup = new RBTree(function (a, b) { return a.key - b.key })
    this.binder = binder
    options.play = this.play.bind(this)
    options.brief = this.brief.bind(this)
    options.url = binder.location
    this.kibitzer = new Kibitzer(options)
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
    console.log('callled')
})

Container.prototype.brief = function (previous) {
    var key = previous || this.lookup.max()
    if (key == null) {
        return { previous: null, entries: [] }
    }
    var iterator = this.lookup.upperBound({ key: key }), entries = [], entry, count = 24
    while (count-- && (entry = iterator.prev())) {
        entries.push({ type: 'add', value: entry })
    }
    return { previous: iterator.data(), entries: entries }
}

module.exports = Container
