## Using the cassandra schema tool
This package contains the tooling for temporal cassandra operations.

## For localhost development
For the very first time run:
``` 
make
```

then run:
``` 
make install-schema
```
to create schama in your `cassandra` instance.

## For production

### Create the binaries
- Run `make`
- You should see an executable `temporal-cassandra-tool`

### Do one time database creation and schema setup for a new cluster
This uses Cassandra's SimpleStratagey for replication. For production, we recommend using a replication factor of 3 with NetworkTopologyStrategy.

```
temporal-cassandra-tool --ep $CASSANDRA_SEEDS create -k $KEYSPACE --rf $RF
```

See https://www.ecyrd.com/cassandracalculator for an easy way to determine how many nodes and what replication factor you will want to use.  Note that Temporal by default uses `Quorum` for read and write consistency.

```
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal update-schema -d ./schema/cassandra/temporal/versioned -- upgrades your schema to the latest version

./temporal-cassandra-tool -ep 127.0.0.1 -k temporal_visibility setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0 for visibility
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal_visibility update-schema -d ./schema/cassandra/visibility/versioned -- upgrades your schema to the latest version for visibility
```

### Update schema as part of a release
You can only upgrade to a new version after the initial setup done above.

```
./temporal-cassandra-tool -ep 127.0.0.1 -k temporal update-schema -d ./schema/cassandra/temporal/versioned -v x.x    -- executes the upgrade to version x.x

./temporal-cassandra-tool -ep 127.0.0.1 -k temporal_visibility update-schema -d ./schema/cassandra/visibility/versioned -v x.x    -- executes the upgrade to version x.x
```

