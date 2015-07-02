var sys = require('sys')
var exec = require('child_process').exec
var os = require('os')

function execOut (error, stdout, stderr) {
    for (arg in arguments) {if (arg != null) console.log(arg)}
}

if (os.platform == 'darwin') {
    exec('$(boot2docker shellinit 2> /dev/null)', function(error) {
        execOut(arguments)
        if (error != null) {
            exec('boot2docker up', execOut)
        }
    })
}

exec('docker pull marvambass/nginx-ssl-secure', execOut)
