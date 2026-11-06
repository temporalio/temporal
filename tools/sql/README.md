## Using the SQL schema tool
 
This package contains the tooling for temporal sql operations. The tooling itself is agnostic of the storage engine behind
the sql interface. So, this same tool can be used against, say, OracleDB and MySQLDB

## For localhost development
``` 
SQL_USER=$USERNAME SQL_PASSWORD=$PASSWD make install-schema-mysql
```

## For production

### Create the binary
- Run `make temporal-sql-tool` on the root of repositroy
- You should see an executable `temporal-sql-tool`
- Temporal officially support MySQL and Postgres for SQL. 
- For other SQL database, you can add it easily as we do for MySQL/Postgres following our code in sql-extensions  

### Do one time database creation and schema setup for a new cluster

- All command below are taking MySQL as example. For postgres, simply use with "--plugin postgres12" and "--schema-name postgresql/v12/temporal" & "--schema-name postgresql/v12/visibility"

```
./temporal-sql-tool --ep $SQL_HOST -p $port --db temporal --plugin mysql8 create
./temporal-sql-tool --ep $SQL_HOST -p $port --db temporal_visibility --plugin mysql8 create
```

```
./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0
./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal update-schema --schema-name mysql/v8/temporal -- upgrades your schema with the embedded schema
./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal update-schema -d ./schema/mysql/v8/temporal/versioned -- Or upgrades your schema by providing your schema

./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal_visibility setup-schema -v 0.0 -- this sets up just the schema version tables with initial version of 0.0 for visibility
./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal_visibility update-schema --schema-name mysql/v8/visibility -- upgrades your schema with the embedded schema for visibility
./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal_visibility update-schema -d ./schema/mysql/v8/visibility/versioned  -- Or upgrades your schema by providing your schema for visibility
```

### Use one MySQL database for core and visibility

- For a new shared database, configure both server datastores to use it and add `--visibility` to visibility setup and updates. This flag is MySQL-only.
- Core metadata uses `schema_version` and `schema_update_history`; visibility uses `visibility_version` and `visibility_update_history`.

```sh
./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal setup-schema -v 0.0
./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal update-schema --schema-name mysql/v8/temporal
./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal setup-schema --visibility -v 0.0
./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal update-schema --visibility --schema-name mysql/v8/visibility
```

- Existing dedicated visibility databases can keep using the original separate-database commands without `--visibility`. The server reads legacy metadata only when `visibility_version` is absent, not when it is empty or outdated.
- To adopt the new names in a dedicated visibility database, stop its services, back it up, and rename its metadata tables. Then use `--visibility` for future updates; older servers do not support the new names.

```sql
RENAME TABLE schema_version TO visibility_version,
             schema_update_history TO visibility_update_history;
```

- Do not rename core metadata in a shared database or rerun initial setup on an existing visibility schema. Moving existing visibility data into a shared database requires a separate migration. `--visibility --overwrite` is rejected to protect other tables.

### Update schema as part of a release
You can only upgrade to a new version after the initial setup done above.

```
./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal update-schema -d ./schema/mysql/v8/temporal/versioned -v x.x    -- executes the upgrade to version x.x

./temporal-sql-tool --ep $SQL_HOST -p $port --plugin mysql8 --db temporal_visibility update-schema -d ./schema/mysql/v8/visibility/versioned -v x.x    -- executes the upgrade to version x.x
```

