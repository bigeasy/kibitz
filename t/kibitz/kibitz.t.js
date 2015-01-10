var cadence = require('cadence')
var UserAgent = require('inlet/http/ua')

require('proof')(3, cadence(prove))

function prove (async, assert) {
    var Kibitzer = require('../..'),
        Container = require('../container'),
        Binder = require('inlet/net/binder'),
        Bouquet = require('inlet/net/bouquet')

    var ua = new UserAgent

    var options = {
    }

    var bouquet = new Bouquet
    var binder = new Binder('http://127.0.0.1:8086')
    var containers = [ new Container(binder, options) ]

    async(function () {
        bouquet.start(containers[0], async())
    }, [function () {
        containers[0].kibitzer.join(binder.location + '/discover', async())
    }, function (_, error) {
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
        console.log(response)
        containers[1].kibitzer.wait('2/0', async())
    }, function () {
        assert(containers[1].kibitzer.legislator.government.majority.length, 2, 'registered second participant')
        ua.fetch({
            url: containers[1].kibitzer.url
        }, {
            url: '/discover'
        }, async())
        console.log('fetched')
    }, function (body, response) {
        console.log(body)
    }, function () {
        console.log(containers[1].kibitzer.legislator.government)
        console.log('stopping')
        bouquet.stop(async())
    })
}
