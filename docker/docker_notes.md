#Boot2Docker

`Boot2Docker` uses a Linux VM as the Docker host. `Boot2Docker` includes `Docker` in its install.
Within the `Boot2Docker` VM, you can issue `docker` commands.

Download here: https://github.com/boot2docker/osx-installer/releases/latest

You can run Boot2Docker from /Applications, but from the command line:
    `boot2docker init` creates a new virtual machine.
    `boot2docker up` starts daemon and prints ports.
    `boot2docker start` does what it says.
    `boot2docker shellinit` sets and displays default ENV variables.
    `docker run hello-world` verifies setup.


#Docker
`docker run` options:
    `-P` publishes exposed ports
    `-d` keeps container running in background

`docker ps` is, er, `ps` for docker. `-a` includes stopped containers, `-l` specifies the last container started.
    `-q` lists just container IDs.

`docker cp` is cp.

`boot2docker ip` gives the VM address.

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

#Docker Compose
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
`ports:
    - "host:container"
`
`expose:
    - "port"
`

#Docker Machine
Machine installation:
`$ curl -L https://github.com/docker/machine/releases/download/v0.2.0/docker-machine_darwin-amd64 > /usr/local/bin/docker-machine
`$ chmod +x /usr/local/bin/docker-machine
"Machine makes it really easy to create Docker hosts on your computer, on cloud providers and inside your own data center. It creates servers, installs Docker on them, then configures the Docker client to talk to them."

`docker-machine create`
optional `--driver` flag, can specify 'virtualbox'

on every start run:
`$ eval "$(docker-machine env dev)"
$ docker ps` to copy env vars
then you can run normal docker commands.
`docker-machine [stop/start]
can specify host as last argument ex: `docker-machine stop dev`

can pass credentials for cloud platforms to `docker create` ex: `--digitalocean-access-token`

ex:
$ docker-machine create \
    --driver digitalocean \
    --digitalocean-access-token 0ab77166d407f479c6701652cee3a46830fef88b8199722b87821621736ab2d4 \
    staging

`docker-machine create -h` to view credential options

Host options:
select active host with `docker-machine active HOSTNAME`
create host without driver (just URL) for aliases:
`$ docker-machine create --url=tcp://50.134.234.20:2376 custombox


#Docker Swarm
native clustering for docker hosts.
`docker pull swarm` to install

$ docker run --rm swarm create <-- creates a swarm cluster, returns a unique `cluster_id`.

For each node:
`$ docker -H tcp://0.0.0.0:2375 -d` <-- start the docker daemon
`docker run -d swarm join --addr=<node_ip:2375> token://<cluster_id>` <--- register nodes

to start the swarm manager:
`docker run -d -p <swarm_port>:2375 swarm manage token://<cluster_id>`

then you can use normal docker commands like so:
`docker -H tcp://<manager_ip:manager_port> info`

swarm list:
`docker run --rm swarm list token://<cluster_id>`

after `docker run swarm create`, you can Machine a swarm.

create swarm master.
`docker-machine create \
    -d virtualbox \
    --swarm \
    --swarm-master \
    --swarm-discovery token://<TOKEN-FROM-ABOVE> \
    swarm-master`

create more nodes.
`docker-machine create \
    -d virtualbox \
    --swarm \
    --swarm-discovery token://<TOKEN-FROM-ABOVE> \
    swarm-node-00`


#ANSIBLE

guides: http://docs.ansible.com/ansible/guides.html
example playbooks: https://github.com/ansible/ansible-examples



