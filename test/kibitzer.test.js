require('proof')(1, async (okay) => {
    const Destructible = require('destructible')
    const destructible = new Destructible('test')

    const Kibitzer = require('..')

    const kibitzers = []

    function _createKibitzer (destructible, id, republic) {
        const kibitzer = new Kibitzer(destructible, {
            republic: republic,
            id: id,
            ua: {
                send: function (envelope) {
                    return kibitzers.filter(function (kibitzer) {
                        return kibitzer.paxos.id == envelope.to.location
                    }).pop().request(JSON.parse(JSON.stringify(envelope)))
                }
            },
            ping: 50,
            timeout: 150
        })
        return kibitzer
    }

    function createKibitzer (id, republic) {
        return _createKibitzer(destructible.ephemeral([ 'kibitzer', 0 ]), id, republic)
    }

    kibitzers.push(createKibitzer('0', 0))
    kibitzers[0].bootstrap(1, { location: '0' })
    const shifter = kibitzers[0].paxos.log.shifter()
    kibitzers.push(createKibitzer('1', 0))
    kibitzers[1].join(1)
    kibitzers[0].embark(1, '1', kibitzers[1].paxos.cookie, { location: '1' })
    await new Promise(resolve => setTimeout(resolve, 100))
    kibitzers.push(createKibitzer('2', 0))
    kibitzers[2].join(1)
    kibitzers[0].embark(1, '2', kibitzers[2].paxos.cookie, { location: '2' })
    const join = kibitzers[2].paxos.log.shifter()
    await join.pump(entry => {
        if (entry != null && entry.promise == '3/0') {
            join.destroy()
        }
    })
    kibitzers[2].acclimate()
    kibitzers[2].publish(1)
    kibitzers[0].publish(1)
    const publish = kibitzers[2].paxos.log.shifter()
    await publish.pump(entry => {
        if (entry != null && entry.promise == '3/1') {
            publish.destroy()
        }
    })
    kibitzers[2].request({
        method: 'enqueue',
        body: { cookie: '1', republic: 0, entries: [ '1' ] }
    })
    destructible.destroy()
    await destructible.destructed
    okay(true, 'complete')
})
