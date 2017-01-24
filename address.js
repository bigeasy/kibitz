function Address (legislator) {
    this._legislator = legislator
    this.spigot = new Spigot(this)
    this.basin = new Basin(this)
}

Address.prototype.request = cadence(function (async, envelope) {
    return {
        to: this._legislator.properties[envelope.body.to],
        from: envelope.from,
        body: envelope.body
    }
})

Address.prototype.response = cadence(function (async, envelope) {
    return envelope
})
