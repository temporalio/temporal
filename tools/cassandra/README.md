## What
This package contains the tooling for cadence cassandra operations.

## How
- Run `make bins`
- You should see an executable `cadence-cassandra-tool`

## Setting up cassandra schema on a new cluster shortcut
``` 
make install-schema
```

## Setting up schema for production use
The default Makefile sets up a replication factor of 1.  This should be used for developer setups when running Cadence server on a laptop. For production clusters we recommend to use a replication factor of 3. Please use 'cadence-cassandra-tool' to create both the cadence and cadence_visibility keyspaces. It already has an option to configure the replication factor.

```
cadence-cassandra-tool --ep $CASSANDRA_SEEDS create -k $KEYSPACE --rf $RF
```

See https://www.ecyrd.com/cassandracalculator for an easy way to determine how many nodes and what replication factor you will want to use.  Note that Cadence by default uses `Quorum` for read and write consistency.

## Setting up schema on a new cluster manually
```
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence update-schema -d ./schema/cassandra/cadence/versioned -- upgrades your schema to the latest version

./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0 for visibility
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility update-schema -d ./schema/cassandra/visibility/versioned -- upgrades your schema to the latest version for visibility
```

## Updating schema on an existing cluster
You can only upgrade to a new version after the initial setup done above.

```
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence update-schema -d ./schema/cassandra/cadence/versioned -v x.x -y -- executes a dryrun of upgrade to version x.x
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence update-schema -d ./schema/cassandra/cadence/versioned -v x.x    -- actually executes the upgrade to version x.x

./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility update-schema -d ./schema/cassandra/visibility/versioned -v x.x -y -- executes a dryrun of upgrade to version x.x
./cadence-cassandra-tool -ep 127.0.0.1 -k cadence_visibility update-schema -d ./schema/cassandra/visibility/versioned -v x.x    -- actually executes the upgrade to version x.x
```

