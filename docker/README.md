Quickstart for localhost development
====================================

Install docker: https://docs.docker.com/engine/installation/

Following steps will bring up the docker container running temporal server
along with all its dependencies (cassandra, statsd, graphite). Exposes temporal
frontend on port 7233 and grafana metrics frontend on port 8080.

```
cd $GOPATH/src/github.com/temporalio/temporal/docker
docker-compose up
```

View metrics at localhost:8080/dashboard    
View Cadence-Web at localhost:8088  
Use Temporal-CLI with `docker run --network=host --rm temporalio/tctl:latest`

For example to register new domain 'test-domain' with 1 retention day
`docker run --network=host --rm temporalio/tctl:latest --do test-domain domain register -rd 1`


Using a pre-built image
-----------------------
With every tagged release of the temporal server, there is also a corresponding
docker image that's uploaded to docker hub. In addition, the release will also
contain a **docker.tar.gz** file (docker-compose startup scripts). 
Go [here](https://github.com/temporalio/temporal/releases/latest) to download a latest **docker.tar.gz** 

Execute the following
commands to start a pre-built image along with all dependencies (cassandra/statsd).

```
tar -xzvf docker.tar.gz
cd docker
docker-compose up
```

Building an image for any branch and restarting
-----------------------------------------
Replace **YOUR_TAG** and **YOUR_CHECKOUT_BRANCH** in the below command to build:
```
cd $GOPATH/src/github.com/temporalio/temporal
git checkout YOUR_CHECKOUT_BRANCH
docker build . -t temporalio/temporal:YOUR_TAG --build-arg TARGET=auto-setup
```
Replace the tag of **image: temporalio/temporal** to **YOUR_TAG** in docker-compose.yml .
Then stop service and remove all containers using the below commands.
```
docker-compose down
docker-compose up
```

Running temporal service with MySQL
-----------------------------------------

Run temporal with MySQL instead of Cassandra, use following commads:

```
docker-compose -f docker-compose-mysql.yml up
docker-compose -f docker-compose-mysql.yml down
```

Please note that SQL support is still in active developement and it is not production ready yet.

Running temporal service with ElasticSearch
-----------------------------------------

Run temporal with ElasticSearch for visibility instead of Cassandra/MySQL

```
docker-compose -f docker-compose-es.yml up
``` 

Quickstart for production
=========================
In a typical production setting, dependencies (cassandra / statsd server) are
managed / started independently of the temporal-server. To use the container in
a production setting, use the following command:


```
docker run -e CASSANDRA_SEEDS=10.x.x.x                  -- csv of cassandra server ipaddrs
    -e KEYSPACE=<keyspace>                              -- Cassandra keyspace
    -e VISIBILITY_KEYSPACE=<visibility_keyspace>        -- Cassandra visibility keyspace
    -e SKIP_SCHEMA_SETUP=true                           -- do not setup cassandra schema during startup
    -e RINGPOP_SEEDS=10.x.x.x,10.x.x.x  \               -- csv of ipaddrs for gossip bootstrap
    -e STATSD_ENDPOINT=10.x.x.x:8125                    -- statsd server endpoint
    -e NUM_HISTORY_SHARDS=1024  \                       -- Number of history shards
    -e SERVICES=history,matching \                      -- Spinup only the provided services
    -e LOG_LEVEL=debug,info \                           -- Logging level
    -e DYNAMIC_CONFIG_FILE_PATH=config/foo.yaml         -- Dynamic config file to be watched
    temporalio/temporal:<tag>
```

