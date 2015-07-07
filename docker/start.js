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
            // boot2docker probably isn't running.
            exec('boot2docker up', execOut)
        }
    })
}

exec('docker pull arvambass/nginx-ssl-secure', execOut)
exec('mkdir ~/kibitz', execOut)
exec('docker run -v ~/kibitz'
