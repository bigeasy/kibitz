var sys = require('sys')
var exec = require('child_process').exec
var os = require('os')
var fs = require('fs')

function execOut (error, stdout, stderr) {
    for (arg in arguments) {if (arg != null) console.log(arguments[arg])}
}

if (os.platform == 'darwin') {
    exec('$(boot2docker shellinit 2> /dev/null)', function(error) {
        execOut(arguments)
        if (error != null) {
            // boot2docker probably isn't running.
            exec('boot2docker up', execOut)
            process.exit(1)
        }
    })
}

exec('mkdir ./kibitz', function () {
    execOut(arguments)
    exec('git clone git@github.com:bigeasy/kibitz.git ./kibitz', function () {
        execOut(arguments)
        fs.symlinkSync('../kibitzer.js', './kibitz/kibitzer.js', execOut)
    })
})

exec('docker pull ubuntu', function () {
    execOut(arguments)
    exec('docker build -t kibitz .', function () {
        execOut(arguments)
        exec('docker run -v ./kibitz ' + os.hostname() + '/kibitz', execOut)
    })
})

