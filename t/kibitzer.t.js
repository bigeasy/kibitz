require('proof')(3, require('cadence')(prove))

function prove (async, okay) {
    var abend = require('abend')
    var cadence = require('cadence')
    var Procession = require('procession')

    var Procedure = require('conduit/procedure')
    var Caller = require('conduit/caller')

    var Destructible = require('destructible')

    var Timer = require('happenstance').Timer

    var Kibitzer = require('..')
    var kibitzers = []

    var shifter

    var destructible = new Destructible('t/kibitzer.t.js')

    destructible.completed.wait(async())

    var createKibitzer = cadence(function (async, destructible, id, republic) {
        async(function () {
            destructible.monitor('procedure', Procedure, cadence(function (async, envelope) {
                kibitzers.filter(function (kibitzer) {
                    return kibitzer.paxos.id == envelope.to.location
                }).pop().request(JSON.parse(JSON.stringify(envelope)), async())
            }), async())
        }, function (procedure) {
            async(function () {
                destructible.monitor('caller', Caller, async())
            }, function (caller) {
                caller.outbox.pump(procedure.inbox)
                procedure.outbox.pump(caller.inbox)
                var kibitzer = new Kibitzer({
                    republic: republic, id: id, caller: caller
                })
                async(function () {
                    destructible.monitor('kibitzer', kibitzer, 'listen', async())
                }, function () {
                    return kibitzer
                })
            })
        })
    })

    async(function () {
        destructible.monitor([ 'kibitzer', 0 ], createKibitzer, '0', 0, async())
    }, function (kibitzer) {
        kibitzers.push(kibitzer)
        okay(kibitzers[0], 'construct')
        kibitzers[0].bootstrap(1, { location: '0' })

        shifter = kibitzers[0].paxos.log.shifter()
    }, function () {
        destructible.monitor([ 'kibitzer', 0 ], createKibitzer, '1', 0, async())
    }, function (kibitzer) {
        kibitzers.push(kibitzer)
        kibitzers[1].join(1)
        kibitzers[0].arrive(1, '1', kibitzers[1].paxos.cookie, { location: '1' })
    }, function () {
        setTimeout(async(), 100)
    }, function () {
        destructible.monitor([ 'kibitzer', 0 ], createKibitzer, '2', 0, async())
    }, function (kibitzer) {
        kibitzers.push(kibitzer)
        kibitzers[2].join(1)
        kibitzers[0].arrive(1, '2', kibitzers[2].paxos.cookie, { location: '2' })
        shifter.join(function (entry) {
            return entry.promise == '3/0'
        }, async())
    }, function () {
        kibitzers[2].acclimate()
        kibitzers[2].publish(1)
        kibitzers[0].publish(1)
        shifter.join(function (entry) {
            return entry.promise == '3/1'
        }, async())
    }, function (entry) {
        okay(entry.body.body, 1, 'published')
        kibitzers[2].request({
            method: 'enqueue',
            body: { cookie: '1', republic: 0, entries: [ '1' ] }
        }, async())
    }, function (response) {
        okay(response, null, 'failed submission')
        destructible.destroy()
    })
}
