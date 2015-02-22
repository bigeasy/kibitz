var cadence = require('cadence/redux')
var UserAgent = require('inlet/http/ua')

require('proof')(7, cadence(prove))

function prove (async, assert) {
    var Kibitzer = require('../..'),
        Container = require('../container'),
        Binder = require('inlet/net/binder'),
        Bouquet = require('inlet/net/bouquet')

    var ua = new UserAgent

    var options = {
    }

    new Kibitzer({}).logger(1) // defaults

    var kibitzer = new Kibitzer({
        logger: function (level, context, error) {
            assert(context, 'test', 'catcher context')
            if (!error) throw new Error
            assert(error.message, 'catcher caught')
        }
    })

    assert(kibitzer._response({ okay: false }, null, null, null, 1), 1, 'not okay')
    assert(kibitzer._response({ okay: true }, { posted: false }, 'posted', null, 1), 1, 'not okay')

    kibitzer.catcher('test')(new Error('caught'))

    var bouquet = new Bouquet
    var binder = new Binder('http://127.0.0.1:8086')
    var containers = [ new Container(binder, options) ]
    var waitFor

    async(function () {
        bouquet.start(containers[0], async())
    }, [function () {
        containers[0].kibitzer.join(binder.location + '/discover', async())
    }, function (error) {
        assert(error.message, 'no other participants', 'no other participants')
    }], function () {
        containers[0].kibitzer.bootstrap(binder.location)
    }, function () {
        var binder = new Binder('http://127.0.0.1:8087')
        containers.push(new Container(binder, options))
        bouquet.start(containers[1], async())
    }, function () {
        containers[1].kibitzer.join(binder.location + '/discover', async())
    }, function (response) {
        assert(containers[1].kibitzer.legislator.government.constituents.length, 1, 'registered first participant')
    }, function () {
        var binder = new Binder('http://127.0.0.1:8088')
        containers.push(new Container(binder, options))
        bouquet.start(containers[2], async())
    }, function () {
        containers[2].kibitzer.join(binder.location + '/discover', async())
    }, function (response) {
        containers[1].kibitzer.wait('2/0', async())
    }, function () {
        assert(containers[1].kibitzer.legislator.government.majority.length, 2, 'registered second participant')
        ua.fetch({
            url: containers[1].kibitzer.url
        }, {
            url: '/discover'
        }, async())
    }, function (body, response) {
        containers[0].kibitzer.publish({ type: 'add', key: 1, value: 'a' }, async())
    }, function () {
        containers[1].kibitzer.wait('2/1', async())
        containers[1].kibitzer.wait('2/1', async())
    }, function () {
        // test waiting for something that has already arrived.
        containers[1].kibitzer.wait('2/1', async())
    }, function () {
        containers[1].lookup.each(function (entry) {
            console.log(entry)
        })
        var i = 0, loop = async(function () {
            if (i++ === 48) return [ loop ]
            async(function () {
                containers[2].kibitzer.publish({ type: 'add', key: i, value: 'a' }, async())
            })
        })()
    }, function () {
        var binder = new Binder('http://127.0.0.1:8089')
        containers.push(new Container(binder, options))
        bouquet.start(containers[3], async())
    }, function () {
        containers[3].kibitzer.join(binder.location + '/discover', async())
    }, function () {
        assert(containers[3].kibitzer.legislator.government.majority.length, 2, 'registered third participant')
        setTimeout(async(), 350)
    }, function () {
        containers.forEach(function (container) {
            container.kibitzer.stop()
        })
        bouquet.stop(async())
    })
}
