## Using the cassandra schema tool
This package contains the tooling for cadence cassandra operations.

## For localhost development
``` 
make install-schema
```

## For production

### Create the binaries
- Run `make bins`
- You should see an executable `cadence-cassandra-tool`

### Do one time database creation and schema setup for a new cluster
This uses Cassandra's SimpleStratagey for replication. For production, we recommend using a replication factor of 3 with NetworkTopologyStrategy.

```
cadence-cassandra-tool --ep $CASSANDRA_SEEDS create -k $KEYSPACE --rf $RF
```

See https://www.ecyrd.com/cassandracalculator for an easy way to determine how many nodes and what replication factor you will want to use.  Note that Cadence by default uses `Quorum` for read and write consistency.

```
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence update-schema -d ./schema/cassandra/cadence/versioned -- upgrades your schema to the latest version

./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0 for visibility
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility update-schema -d ./schema/cassandra/visibility/versioned -- upgrades your schema to the latest version for visibility
```

### Update schema as part of a release
You can only upgrade to a new version after the initial setup done above.

```
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence update-schema -d ./schema/cassandra/cadence/versioned -v x.x -y -- executes a dryrun of upgrade to version x.x
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence update-schema -d ./schema/cassandra/cadence/versioned -v x.x    -- actually executes the upgrade to version x.x

./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility update-schema -d ./schema/cassandra/visibility/versioned -v x.x -y -- executes a dryrun of upgrade to version x.x
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility update-schema -d ./schema/cassandra/visibility/versioned -v x.x    -- actually executes the upgrade to version x.x
```

