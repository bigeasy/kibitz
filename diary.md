#Boot2Docker

`Boot2Docker` uses a Linux VM as the Docker host. `Boot2Docker` includes `Docker` in its install.
Within the `Boot2Docker` VM, you can issue `docker` commands.

Download here: https://github.com/boot2docker/osx-installer/releases/latest

You can run Boot2Docker from /Applications, but from the command line:
    `boot2docker init` creates a new virtual machine.
    `boot2docker start` does what it says.
    `boot2docker shellinit` sets and displays default ENV variables.
    `docker run hello-world` verifies setup.

`docker run` options:
    `-P` publishes exposed ports
    `-d` keeps container running in background

`docker ps` is, er, `ps` for docker. `-a` includes stopped containers, `-l` specifies the last container started.

`boot2docker ip` gives the VM address.

can `docker search` for and `docker pull` images.

To create an image, touch a `Dockerfile` and run
```
docker --tls build -t <your username>/<app name> .
```

`stop`, `rm`, `rmi`, `build` <<< container/image commands

 `docker inspect` returns JSON that can be formatted for the desired output.
you can use `--format=''` to specify a field ex. `docker inspect --format='\{{.LogPath}}'

docker only uses environment vars and /etc/hosts to communicate between linked containers.
link with `--link` and a container name.

data volumes lend persistent, container-agnostic data. can be shared and reused across containers. add '-v' and a directory to `create` or `run`.

Compose lets you define multi-container applications in a Dockerfile.
you also need a `docker-compose.yml` to define your app's services.
to run: `docker-compose up`

Compose installation:
`curl -L https://github.com/docker/compose/releases/download/1.2.0/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose`
`chmod +x /usr/local/bin/docker-compose`

`docker-compose run [cmd]` lets you run commands on your services.
`docker-compose stop`.

compose.yml:
`links:
    - service:alias
or
    - alias
`
`external_links:
    - container:alias
or
    - alias
`
