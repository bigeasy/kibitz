require('proof/redux')(2, require('cadence')(prove))

function prove (async, assert) {
    var cadence = require('cadence')

    var Procession = require('procession')
    var Responder = require('conduit/responder')

    var Kibitzer = require('..')
    var kibitzers = []

    kibitzers.push(createKibitzer())
    assert(kibitzers[0], 'construct')
    kibitzers[0].bootstrap(0)

    var shifter = kibitzers[0].log.shifter()

    async(function () {
        kibitzers.push(createKibitzer())
        kibitzers[1].join({ republic: 0, location: '0' }, async())
    }, function () {
        kibitzers.push(createKibitzer())
        kibitzers[2].join({ republic: 0, location: '1' }, async())
    }, function () {
        shifter.join(function (entry) { return entry.body.body == 1 }, async())
        kibitzers[2].publish(1)
    }, function (entry) {
        assert(entry.body.body, 1, 'published')
        kibitzers.forEach(function (kibitzer) { kibitzer.shutdown(async()) })
    }, function () {
        kibitzers.forEach(function (kibitzer) { kibitzer.shutdown(async()) })
    })


    function createKibitzer () {
        var responder = new Responder({
            request: cadence(function (async, envelope) {
                kibitzers.filter(function (kibitzer) {
                    return kibitzer.properties.location == envelope.to.location
                }).pop().request(envelope, async())
            })
        }, 'kibitz')
        var id = String(kibitzers.length)
        var kibitzer = new Kibitzer({ id: id, properties: { location: id } })
        kibitzer.spigot.emptyInto(responder.basin)
        return kibitzer
    }
}
