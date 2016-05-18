require('proof')(4, require('cadence')(prove))

function prove (async, assert) {
    var UserAgent  = require('../ua')
    var http = require('http')
    var Semblance = require('semblance')
    var semblance = new Semblance
    var server = http.createServer(semblance.dispatch()), request
    var ua = new UserAgent('http://127.0.0.1:8887/discover')
    async(function () {
        server.listen(8887, async())
    }, function () {
        ua.discover(async())
    }, function () {
        assert(semblance.shift().url, '/discover', 'discover')
        ua.sync('http://127.0.0.1:8887', {}, async())
    }, function () {
        assert(semblance.shift().url, '/sync', 'sync')
        ua.enqueue('http://127.0.0.1:8887', {}, async())
    }, function () {
        assert(semblance.shift().url, '/enqueue', 'enqueue')
        ua.receive('http://127.0.0.1:8887', {}, async())
    }, function () {
        assert(semblance.shift().url, '/receive', 'receive')
    }, function () {
        server.close(async())
    })
}
