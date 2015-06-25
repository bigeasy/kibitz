var sys = require('sys')
var exec = require('child_process').exec

exec('docker pull marvambass/nginx-ssl-secure', function(error, stdout){
    console.log(stdout)
});
