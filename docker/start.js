var sys = require('sys')
var exec = require('child_process').exec
var os = require('os')


if (os.platform == 'darwin') {
    exec('$(boot2docker shellinit 2> /dev/null)', function(error, stdout) {
        for (arg in arguments) {if (arg) console.log(arg)}
    })
}

exec('docker pull marvambass/nginx-ssl-secure', function(error, stdout){
    console.log(stdout)
});
