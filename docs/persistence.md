# Overview
Temporal has a well defined API interface at the persistence layer. Any database that supports multi-row transactions on
a single shard or partition can be made to work with temporal. This includes cassandra, dynamoDB, auroraDB, MySQL,
Postgres and may others. There are currently three supported database implementations at the persistence layer - 
cassandra and MySQL/Postgres. This doc shows how to run temporal with cassandra and MySQL(Postgres is mostly the same). It also describes the steps involved
in adding support for a new database at the persistence layer.
 
# Getting started on mac
## Cassandra
### Start cassandra
```
brew install cassandra
brew services start cassandra
```
### Install temporal schema
```
cd $GOPATH/github.com/temporalio/temporal
make install-schema
```

### Start temporal server
```
cd $GOPATH/github.com/temporalio/temporal
./temporal-server start --services=frontend,matching,history,worker
```  
 
## MySQL
### Start MySQL server
```
brew install mysql
brew services start mysql
```
### Install temporal schema
```
cd $GOPATH/github.com/temporalio/temporal
make install-schema-mysql
```

### Start temporal server
```
cd $GOPATH/github.com/temporalio/temporal
cp config/development_mysql.yaml config/development.yaml
./temporal-server start --services=frontend,matching,history,worker
```

# Configuration
## Common to all persistence implementations
There are two major sub-subsystems within temporal that need persistence - temporal-core and visibility. temporal-core is
the workflow engine that uses persistence to store state tied to namespaces, workflows, workflow histories, task lists 
etc. temporal-core powers almost all of the temporal APIs. temporal-core could be further broken down into multiple 
subs-systems that have slightly different persistence workload characteristics. But for the purpose of simplicity, we 
don't expose these sub-systems today but this may change in future. Visibility is the sub-system that powers workflow 
search. This includes APIs such as ListOpenWorkflows and ListClosedWorkflows. Today, it is possible to run a temporal 
server with temporal-core backed by one database and temporal-visibility backed by another kind of database.To get the full 
feature set of visibility, the recommendation is to use elastic search as the persistence layer. However, it is also possible 
to run visibility with limited feature set against Cassandra or MySQL today.  The top level persistence configuration looks 
like the following:
 

```
persistence:
  defaultStore: datastore1    -- Name of the datastore that powers temporal-core
  visibilityStore: datastore2 -- Name of the datastore that powers temporal-visibility
  numHistoryShards: 1024      -- Number of temporal history shards, this limits the scalability of single temporal cluster
  datastores:                 -- Map of datastore-name -> datastore connection params
    datastore1:
      cassandra:
         ...
    datastore2:
      sql:
        ...
```

## Note on numHistoryShards
Internally, temporal uses shards to distribute workflow ownership across different hosts. Shards are necessary for the 
horizontal scalability of temporal service. The number of shards for a temporal cluster is picked at cluster provisioning
time and cannot be changed after that. One way to think about shards is the following - if you have a cluster with N
shards, then temporal cluster can be of size 1 to N. But beyond N, you won't be able to add more hosts to scale. In future,
we may add support to dynamically split shards but this is not supported as of today. Greater the number of shards,
greater the concurrency and horizontal scalability.

## Cassandra
```
persistence:
  ...
  datastores:
    datastore1:
      cassandra:
        hosts: "127.0.0.1,127.0.0.2"  -- CSV of cassandra hosts to connect to 
        User: "user-name"
        Password: "password"
        keyspace: "temporal"           -- Name of the cassandra keyspace
        datacenter: "us-east-1a"      -- Cassandra datacenter filter to limit queries to a single dc (optional)
        maxQPS: 1000                  -- MaxQPS to cassandra from a single temporal sub-system on one host (optional)
        maxConns: 2                   -- Number of tcp conns to cassandra server (single sub-system on one host) (optional)
```

## MySQL
The default isolation level for MySQL is READ-COMMITTED. For MySQL 5.7.20 and below only, the isolation level needs to be
specified explicitly in the config via connectAttributes.
 
```
persistence:
  ...
  datastores:
    datastore1:
      sql:
        pluginName: "mysql"            -- name of the go sql plugin
        databaseName: "temporal"        -- name of the database to connect to
        connectAddr: "127.0.0.1:3306"  -- connection address, could be ip address or namespace socket
        connectProtocol: "tcp"         -- connection protocol, tcp or anything that SQL Data Source Name accepts
        user: "temporal" 
        password: "temporal"
        maxConns: 20                   -- max number of connections to sql server from one host (optional)
        maxIdleConns: 20               -- max number of idle conns to sql server from one host (optional)
        maxConnLifetime: "1h"          -- max connection lifetime before it is discarded (optional)
        maxQPS: 1000                   -- max qps to sql server from one host (optional)
        connectAttributes:             -- custom dsn attributes, map of key-value pairs
          tx_isolation: "READ-COMMITTED"   -- required only for mysql 5.7.20 and below, optional otherwise
```

# Adding support for new database

## For Any Database
Temporal can only work against a database that supports multi-row single shard transactions. The top level
persistence API interface can be found [here](https://github.com/temporalio/temporal/blob/master/common/persistence/dataInterfaces.go).
Currently this is only implemented with Cassandra. 

## For SQL Database
As there are many shared concepts and functionalities in SQL database, we abstracted those common code so that is much easier to implement persistence interfaces with any SQL database. It requires your database supports SQL operations like explicit transaction(with pessimistic locking)

This interface is tied to a specific schema i.e. the way data is laid out across tables and the table
names themselves are fixed. However, you get the flexibility wrt how you store the data within a table (i.e. column names and
types are not fixed). The API interface can be found [here](https://github.com/temporalio/temporal/blob/master/common/persistence/sql/plugins/interfaces.go).
It's basically a CRUD API for every table in the schema. A sample schema definition for mysql that uses this interface
can be found [here](https://github.com/temporalio/temporal/blob/master/schema/mysql/v57/temporal/schema.sql)

Any database that supports this interface can be plugged in with temporal server. 
We have implemented Postgres within the repo, and also here is [**an example**](https://github.com/longquanzheng/cadence-extensions/tree/master/cadence-sqlite) to implement any database externally. 
