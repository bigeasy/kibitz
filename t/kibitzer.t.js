require('proof/redux')(2, require('cadence')(prove))

function prove (async, assert) {
    var cadence = require('cadence')
    var Procession = require('procession')

    var Kibitzer = require('..')
    var kibitzers = []

    function Transform (transformer) {
        if (transformer.length == 2) {
            this._transformer = transformer
        } else {
            this._transformer = function (value, callback) {
                transformer(value)
                callback()
            }
        }
        this.outbox = new Procession
    }

    Transform.prototype.enqueue = cadence(function (async, value) {
        async(function () {
            this._transformer.call(null, value, async())
        }, function (value) {
            this.outbox.enqueue(value, async())
        })
    })

    kibitzers.push(createKibitzer())
    assert(kibitzers[0], 'construct')
    kibitzers[0].bootstrap('x')

    var consumer = kibitzers[0].log.consumer()

    async(function () {
        kibitzers.push(createKibitzer())
        kibitzers[1].join({ islandId: 'x', location: '0' }, async())
    }, function () {
        kibitzers.push(createKibitzer())
        kibitzers[2].join({ islandId: 'x', location: '1' }, async())
    }, function () {
        consumer.join(function (entry) {
            return entry.value.value && entry.value.value.value == 1
        }, async())
        kibitzers[2].publish({ value: 1 })
    }, function (entry) {
        assert(entry.value.value, { value: 1 }, 'published')
        kibitzers.forEach(function (kibitzer) { kibitzer.shutdown(async()) })
    }, function () {
        kibitzers.forEach(function (kibitzer) { kibitzer.shutdown(async()) })
    })

    function createKibitzer () {
        var transform = new Transform(cadence(function (async, envelope) {
            async(function () {
                var json = JSON.parse(JSON.stringify(envelope.body))
                kibitzers.filter(function (kibitzer) {
                    return kibitzer.properties.location == envelope.to.location
                }).pop().dispatch(json, async())
            }, function (response) {
                return { to: envelope.from, body: response }
            })
        }))
        var id = String(kibitzers.length)
        var kibitzer = new Kibitzer({ id: id, properties: { location: id } })
        kibitzer.messenger.outbox.pump(transform).outbox.pump(kibitzer.messenger.inbox)
        return kibitzer
    }
}
