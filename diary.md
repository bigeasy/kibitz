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

`docker ps` is, er, `ps` for docker

`boot2docker ip` gives the VM address.
