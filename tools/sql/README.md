## Using the SQL schema tool
 
This package contains the tooling for cadence sql operations. The tooling itself is agnostic of the storage engine behind
the sql interface. So, this same tool can be used against, say, OracleDB and MySQLDB

## For localhost development
``` 
SQL_USER=$USERNAME SQL_PASSWORD=$PASSWD make install-schema-mysql
```

## For production

### Create the binaries
- Run `make bins`
- You should see an executable `cadence-sql-tool`
- Cadence officially support MySQL and Postgres for SQL. 
- For other SQL database, you can add it easily as we do for MySQL/Postgres following our code in sql-extensions  

### Do one time database creation and schema setup for a new cluster
- All command below are taking MySQL as example. For postgres, simply use with "--plugin postgres"

```
cadence-sql-tool --ep $SQL_HOST_ADDR -p $port create --plugin mysql --db cadence
cadence-sql-tool --ep $SQL_HOST_ADDR -p $port create --plugin mysql --db cadence_visibility
```

```
./cadence-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db cadence setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0
./cadence-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db cadence update-schema -d ./schema/mysql/v57/cadence/versioned -- upgrades your schema to the latest version

./cadence-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db cadence_visibility setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0 for visibility
./cadence-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db cadence_visibility update-schema -d ./schema/mysql/v57/visibility/versioned  -- upgrades your schema to the latest version for visibility
```

### Update schema as part of a release
You can only upgrade to a new version after the initial setup done above.

```
./cadence-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db cadence update-schema -d ./schema/mysql/v57/cadence/versioned -v x.x -y -- executes a dryrun of upgrade to version x.x
./cadence-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db cadence update-schema -d ./schema/mysql/v57/cadence/versioned -v x.x    -- actually executes the upgrade to version x.x

./cadence-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db cadence_visibility update-schema -d ./schema/mysql/v57/cadence/versioned -v x.x -y -- executes a dryrun of upgrade to version x.x
./cadence-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db cadence_visibility update-schema -d ./schema/mysql/v57/cadence/versioned -v x.x    -- actually executes the upgrade to version x.x
```

