## Using the SQL schema tool
 
This package contains the tooling for temporal sql operations. The tooling itself is agnostic of the storage engine behind
the sql interface. So, this same tool can be used against, say, OracleDB and MySQLDB

## For localhost development
``` 
SQL_USER=$USERNAME SQL_PASSWORD=$PASSWD make install-schema-mysql
```

## For production

### Create the binaries
- Run `make`
- You should see an executable `temporal-sql-tool`
- Temporal officially support MySQL and Postgres for SQL. 
- For other SQL database, you can add it easily as we do for MySQL/Postgres following our code in sql-extensions  

### Do one time database creation and schema setup for a new cluster
- All command below are taking MySQL as example. For postgres, simply use with "--plugin postgres"

```
temporal-sql-tool --ep $SQL_HOST_ADDR -p $port --db temporal --plugin mysql create
temporal-sql-tool --ep $SQL_HOST_ADDR -p $port --db temporal_visibility --plugin mysql create
```

```
./temporal-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db temporal setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0
./temporal-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db temporal update-schema -d ./schema/mysql/v57/temporal/versioned -- upgrades your schema to the latest version

./temporal-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db temporal_visibility setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0 for visibility
./temporal-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db temporal_visibility update-schema -d ./schema/mysql/v57/visibility/versioned  -- upgrades your schema to the latest version for visibility
```

### Update schema as part of a release
You can only upgrade to a new version after the initial setup done above.

```
./temporal-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db temporal update-schema -d ./schema/mysql/v57/temporal/versioned -v x.x    -- executes the upgrade to version x.x

./temporal-sql-tool --ep $SQL_HOST_ADDR -p $port --plugin mysql --db temporal_visibility update-schema -d ./schema/mysql/v57/visibility/versioned -v x.x    -- executes the upgrade to version x.x
```

