require('proof')(3, require('cadence')(prove))

function prove (async, assert) {
    var abend = require('abend')
    var cadence = require('cadence')
var Procession = require('procession')
    var Responder = require('conduit/responder')

    var Timer = require('happenstance').Timer

    var Kibitzer = require('..')
    var kibitzers = []

    kibitzers.push(createKibitzer('0', 0))
    assert(kibitzers[0], 'construct')
    kibitzers[0].bootstrap(1, { location: '0' })

    var shifter = kibitzers[0].log.shifter()

    async(function () {
        kibitzers.push(createKibitzer('1', 0))
        kibitzers[1].join(1, { location: '0' }, { location: '1' }, async())
    }, function () {
        setTimeout(async(), 100)
    }, function () {
        kibitzers.push(createKibitzer('2', 0))
        kibitzers[2].join(1, { location: '1' }, { location: '2' }, async())
    }, function () {
        setTimeout(async(), 100)
    }, function () {
        kibitzers[2].naturalize()
        shifter.join(function (entry) { return entry.body.body == 1 }, async())
        kibitzers[2].publish(1)
    }, function (entry) {
        assert(entry.body.body, 1, 'published')
        kibitzers[2].request({
            method: 'enqueue',
            body: { cookie: '1', republic: 0, entries: [ '1' ] }
        }, async())
    }, function (response) {
        assert(response, null, 'failed submission')
        kibitzers.forEach(function (kibitzer) { kibitzer.destroy() })
    }, function () {
        kibitzers.forEach(function (kibitzer) { kibitzer.destroy() })
    })


    function createKibitzer (id, republic) {
        var kibitzer = new Kibitzer({ republic: republic, id: id })
        var responder = new Responder({
            request: cadence(function (async, envelope) {
                kibitzers.filter(function (kibitzer) {
                    return kibitzer.paxos.id == envelope.to.location
                }).pop().request(JSON.parse(JSON.stringify(envelope)), async())
            })
        })
        kibitzer.read.shifter().pump(responder.write, 'enqueue')
        responder.read.shifter().pump(kibitzer.write, 'enqueue')
        kibitzer.listen(abend)
        kibitzer.paxos.scheduler.events.shifter().pump(new Timer(kibitzer.paxos.scheduler), 'enqueue')
        return kibitzer
    }
}
