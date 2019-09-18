# Overview
Cadence has a well defined API interface at the persistence layer. Any database that supports multi-row transactions on
a single shard or partition can be made to work with cadence. This includes cassandra, dynamoDB, auroraDB, MySQL,
Postgres and may others. There are currently two supported database implementations at the persistence layer - 
cassandra and MySQL. This doc shows how to run cadence with cassandra and mysql. It also describes the steps involved
in adding support for a new database at the persistence layer.
 
# Getting started on mac
## Cassandra
### Start cassandra
```
brew install cassandra
brew services start cassandra
```
### Install cadence schema
```
cd $GOPATH/github.com/uber/cadence
make install-schema
```

### Start cadence server
```
cd $GOPATH/github.com/uber/cadence
./cadence-server start --services=frontend,matching,history,worker
```  
 
## MySQL
### Start MySQL server
```
brew install mysql
brew services start mysql
```
### Install cadence schema
```
cd $GOPATH/github.com/uber/cadence
make install-schema-mysql
```

### Start cadence server
```
cd $GOPATH/github.com/uber/cadence
cp config/development_mysql.yaml config/development.yaml
./cadence-server start --services=frontend,matching,history,worker
```

# Configuration
## Common to all persistence implementations
There are two major sub-subsystems within cadence that need persistence - cadence-core and visibility. cadence-core is
the workflow engine that uses persistence to store state tied to domains, workflows, workflow histories, task lists 
etc. cadence-core powers almost all of the cadence APIs. cadence-core could be further broken down into multiple 
subs-systems that have slightly different persistence workload characteristics. But for the purpose of simplicity, we 
don't expose these sub-systems today but this may change in future. Visibility is the sub-system that powers workflow 
search. This includes APIs such as ListOpenWorkflows and ListClosedWorkflows. Today, it is possible to run a cadence 
server with cadence-core backed by one database and cadence-visibility backed by another kind of database.To get the full 
feature set of visibility, the recommendation is to use elastic search as the persistence layer. However, it is also possible 
to run visibility with limited feature set against cassandra or mysql today.  The top level persistence configuration looks 
like the following:
 

```
persistence:
  defaultStore: datastore1    -- Name of the datastore that powers cadence-core
  visibilityStore: datastore2 -- Name of the datastore that powers cadence-visibility
  numHistoryShards: 1024      -- Number of cadence history shards, this limits the scalability of single cadence cluster
  datastores:                 -- Map of datastore-name -> datastore connection params
    datastore1:
      cassandra:
         ...
    datastore2:
      sql:
        ...
```

## Note on numHistoryShards
Internally, cadence uses shards to distribute workflow ownership across different hosts. Shards are necessary for the 
horizontal scalability of cadence service. The number of shards for a cadence cluster is picked at cluster provisioning
time and cannot be changed after that. One way to think about shards is the following - if you have a cluster with N
shards, then cadence cluster can be of size 1 to N. But beyond N, you won't be able to add more hosts to scale. In future,
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
        keyspace: "cadence"           -- Name of the cassandra keyspace
        consistency: "QUORUM"         -- Default consistence, MUST be QUORUM for production (optional)
        datacenter: "us-east-1a"      -- Cassandra datacenter filter to limit queries to a single dc (optional)
        maxQPS: 1000                  -- MaxQPS to cassandra from a single cadence sub-system on one host (optional)
        maxConns: 2                   -- Number of tcp conns to cassandra server (single sub-system on one host) (optional)
```

## MySQL
The default isolation level for MySQL is READ-COMMITTED. For MySQL 5.6 and below only, the isolation level needs to be 
specified explicitly in the config via connectAttributes.
 
```
persistence:
  ...
  datastores:
    datastore1:
      sql:
        driverName: "mysql"            -- name of the go sql driver, mysql is the only supported driver today
        databaseName: "cadence"        -- name of the database to connect to
        connectAddr: "127.0.0.1:3306"  -- connection address, could be ip address or domain socket
        connectProtocol: "tcp"         -- connection protocol, tcp or anything that SQL Data Source Name accepts
        user: "uber" 
        password: "uber"
        maxConns: 20                   -- max number of connections to sql server from one host (optional)
        maxIdleConns: 20               -- max number of idle conns to sql server from one host (optional)
        maxConnLifetime: "1h"          -- max connection lifetime before it is discarded (optional)
        maxQPS: 1000                   -- max qps to sql server from one host (optional)
        connectAttributes:             -- custom dsn attributes, map of key-value pairs
          tx_isolation: "READ-COMMITTED"   -- required only for mysql 5.6 and below, optional otherwise
```

# Adding support for new database
As mentioned before, cadence can only work against a database that supports multi-row single shard transactions. The top level
persistence API interface can be found [here](https://github.com/uber/cadence/blob/master/common/persistence/dataInterfaces.go).
Any database that supports this interface can be plugged in with cadence server. For databases that don't support SQL
interface, implementing this interface is the only way to make it work with cadence. 

## Databases that support SQL interface
If your database supports SQL interface, there is a simpler low level API interface that can be implemented to make it
work with cadence. This interface is tied to a specific schema i.e. the way data is laid out across tables and the table
names themselves are fixed. However, you get the flexibility wrt how you store the data within a table (i.e. column names and
types are not fixed). The API interface can be found [here](https://github.com/uber/cadence/blob/master/common/persistence/sql/storage/sqldb/interfaces.go).
It's basically a CRUD API for every table in the schema. A sample schema definition for mysql that uses this interface
can be found [here](https://github.com/uber/cadence/blob/master/schema/mysql/v57/cadence/schema.sql)
