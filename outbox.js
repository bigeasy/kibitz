var Cliffhanger = require('cliffhanger')
var abend = require('abend')
var cadence = require('cadence')
var Queue = require('procession')

function Outbox () {
    this.outbox = new Queue
    this.inbox = new Queue
    this._inbox = this.inbox.consumer()
    this._cliffhanger = new Cliffhanger
    this._receive(abend)
}

Outbox.prototype._receive = cadence(function (async) {
    async(function () {
        this._inbox.dequeue(async())
    }, function (envelope) {
        this._cliffhanger.resolve(envelope.to, [ null, envelope.body ])
    })()
})

Outbox.prototype.send = cadence(function (async, envelope) {
    this.outbox.enqueue({
        from: this._cliffhanger.invoke(async()),
        to: envelope.to,
        body: envelope.body
    }, async())
})

module.exports = Outbox
