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
            process.exit(1)
        }
    })
}

exec('docker pull arvambass/nginx-ssl-secure', execOut)
exec('mkdir ~/kibitz', execOut)
exec('git clone git@github.com:bigeasy/kibitz.git ~/kibitz')
exec('docker build -t kibitz .')
//exec('docker run -v ~/kibitz')
