Quickstart for localhost development
====================================

Install docker: https://docs.docker.com/engine/installation/

Following steps will bring up the docker container running cadence server
along with all its dependencies (cassandra, statsd, graphite). Exposes cadence
frontend on port 7933 and grafana metrics frontend on port 8080.

```
cd $GOPATH/src/github.com/uber/cadence/docker
docker-compose up
```

View metrics at localhost:8080/dashboard


Using a pre-built image
-----------------------
With every tagged release of the cadence server, there is also a corresponding
docker image that's uploaded to docker hub. In addition, the release will also
contain a docker.tar.gz file (docker-compose startup scripts). Execute the following
commands to start a pre-built image along with all dependencies (cassandra/statsd).

```
wget https://github.com/uber/cadence/releases/download/v0.1.0-beta/docker.tar.gz
tar -xzvf docker.tar.gz
cd docker
docker-compose up
```

Updating an existing image and restarting
-----------------------------------------
```
cd $GOPATH/src/github.com/uber/cadence/docker
docker-compose stop
docker-compose build
docker-compose up
```

Quickstart for production
=========================
In a typical production setting, dependencies (cassandra / statsd server) are
managed / started independently of the cadence-server. To use the container in
a production setting, use the following command:


```
docker run -e CASSANDRA_CONSISTENCY=Quorum \            -- Default cassandra consistency level
    -e CASSANDRA_SEEDS=10.x.x.x                         -- csv of cassandra server ipaddrs
    -e KEYSPACE=<keyspace>                              -- Cassandra keyspace
    -e VISIBILITY_KEYSPACE=<visibility_keyspace>        -- Cassandra visibility keyspace
    -e SKIP_SCHEMA_SETUP=true                           -- do not setup cassandra schema during startup
    -e RINGPOP_SEEDS=10.x.x.x  \                        -- csv of ipaddrs for gossip bootstrap
    -e STATSD_ENDPOINT=10.x.x.x:8125                    -- statsd server endpoint
    -e NUM_HISTORY_SHARDS=1024  \                       -- Number of history shards
    -e SERVICES=history,matching \                      -- Spinup only the provided services
    ubercadence/server:<tag>
```
