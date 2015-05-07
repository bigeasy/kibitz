#Boot2Docker

`Boot2Docker` uses a Linux VM as the Docker host. `Boot2Docker` includes `Docker` in its install.
Within the `Boot2Docker` VM, you can issue `docker` commands.

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
you can use `--format=''` to specify a field ex: `docker inspect --format='{{.LogPath}}'
