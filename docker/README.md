Quickstart for localhost development
====================================

Install [docker](https://docs.docker.com/engine/installation/) and [docker-compose](https://docs.docker.com/compose/install/).

The following steps will bring up a docker container that is running the Temporal server
and its dependencies (cassandra). The Temporal
frontend is exposed on port `7233`.

```bash
$ cd docker
$ docker-compose up
```

View Temporal web UI at `localhost:8088`.
Use Temporal CLI with `docker run --network=host --rm temporalio/tctl:latest`.

For example to register new namespace `test-namespace` with 1 retention day:
```bash
docker run --network=host --rm temporalio/tctl:latest --ns test-namespace namespace register -rd 1`
```

Using a pre-built image
-----------------------
With every tagged release of the temporal server, there is also a corresponding
docker image that's uploaded to docker hub. In addition, the release will also
contain a **docker.tar.gz** file (docker-compose startup scripts).
[Download](https://github.com/temporalio/temporal/releases/latest) the latest **docker.tar.gz**.

Execute the following
commands to start a pre-built image along with all dependencies (i.e. `cassandra`, `mysql`, `elasticsearch`, etc).

```bash
$ curl -L https://github.com/temporalio/temporal/releases/latest/download/docker.tar.gz | tar -xz
$ cd docker
$ docker-compose up
```

Building an image for any branch and restarting
-----------------------------------------
Replace **YOUR_TAG** and **YOUR_CHECKOUT_BRANCH** in the below command to build:
```bash
$ git checkout YOUR_CHECKOUT_BRANCH
$ docker build . -t temporalio/auto-setup:YOUR_TAG --build-arg TARGET=auto-setup
```
Replace the tag of **image: temporalio/auto-setup** to **YOUR_TAG** in `docker-compose.yml`.
Then restart the service using the below commands:
```bash
$ docker-compose down
$ docker-compose up
```

Running Temporal service with MySQL
-----------------------------------------

Run Temporal with MySQL instead of Cassandra, use following commads:

```bash
$ docker-compose -f docker-compose-mysql.yml up
```

Please note that PostreSQL support is still in active developement, and it is not production ready yet.

Running Temporal service with ElasticSearch
-----------------------------------------

Run Temporal with ElasticSearch for enhanced visibility queries:

```bash
$ docker-compose -f docker-compose-es.yml up
```

Quickstart for production
=========================
In a typical production setting, dependencies (`cassandra`) are
managed/started independently of the `temporal-server`. To use the container in
a production setting, use the following command:

```bash
$ docker run -e CASSANDRA_SEEDS=10.x.x.x                  -- csv of cassandra server ipaddrs
    -e KEYSPACE=<keyspace>                              -- Cassandra keyspace
    -e VISIBILITY_KEYSPACE=<visibility_keyspace>        -- Cassandra visibility keyspace
    -e SKIP_SCHEMA_SETUP=true                           -- do not setup cassandra schema during startup
    -e RINGPOP_SEEDS=10.x.x.x,10.x.x.x  \               -- csv of ipaddrs for gossip bootstrap
    -e NUM_HISTORY_SHARDS=1024  \                       -- Number of history shards
    -e SERVICES=history,matching \                      -- Spinup only the provided services
    -e LOG_LEVEL=debug,info \                           -- Logging level
    -e DYNAMIC_CONFIG_FILE_PATH=config/foo.yaml         -- Dynamic config file to be watched
    temporalio/server:<tag>
```
