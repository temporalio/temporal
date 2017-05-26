Quickstart
==========

Following steps will bring up the docker container running cadence server
along with all its dependencies (cassandra, statsd, graphite). Exposes cadence
frontend on port 7933 and grafana metrics frontend on port 8080.

```
cd $GOPATH/src/github.com/uber/cadence/docker
docker-compose up
```

View metrics at localhost:8080/dashboard

Building and running the image
==============================

Build
-----
```
cd $GOPATH/src/github.com/uber/cadence/docker
docker-compose build
```

Run with defaults
-----------------
```
docker-compose run cadence
```

Run with all the config options
-------------------------------
```
docker-compose run -e CASSANDRA_CONSISTENCY=Quorum \    -- Default cassandra consistency level
    -e BIND_ON_LOCALHOST=false \                        -- Don't use localhost ip address for cadence services
    -e RINGPOP_SEEDS=10.0.0.1 \                         -- Use this as the gossip bootstrap hosts for ringpop
    -e NUM_HISTORY_SHARDS=1024  \                       -- Number of history shards
    -e SERVICES=history,matching \                      -- Spinup only the provided services
    cadence
```

Running cadence without dependencies
====================================
If you prefer to spin up your cassandra / statsd server, use the following
to just bring up cadence server
```
cd $GOPATH/src/github.com/uber/cadence/docker
docker build -t uber/cadence:master .
docker run uber/cadence
    -e CASSANDRA_CONSISTENCY=Quorum \
    -e CASSANDRA_SEEDS=127.0.0.1 \                      -- Cassandra server seed list
    -e RINGPOP_SEEDS=10.0.0.1 \                         -- Use this as the gossip bootstrap hosts for ringpop
    -e NUM_HISTORY_SHARDS=1024  \                       -- Number of history shards
```