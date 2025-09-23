## Using the Elasticsearch schema tool
 
This package contains the tooling for temporal elasticsearch operations. 

### Usage
```
NAME:
   temporal-elasticsearch-tool - Command line tool for temporal elasticsearch operations

USAGE:
   temporal-elasticsearch-tool [global options] command [command options] [arguments...]

VERSION:
   0.0.1

COMMANDS:
   setup-schema, setup  setup initial version of elasticsearch schema and index
   ping                 pings the elasticsearch host
   help, h              Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --endpoint value, -e value            hostname or ip address of elasticsearch server (default: "http://127.0.0.1:9200") [$ES_SERVER]
   --user value, -u value                username for elasticsearch or aws_access_key_id if using static aws credentials [$ES_USER]
   --password value, -p value            password for elasticsearch or aws_secret_access_key if using static aws credentials [$ES_PWD]
   --es-version value                    elasticsearch version (default: "v7") [$ES_VERSION]
   --aws-credentials value, --aws value  AWS credentials provider (supported ['static', 'environment', 'aws-sdk-default']) [$AWS_CREDENTIALS]
   --aws-session-token value             AWS session token for use with 'static' AWS credentials provider [$AWS_SESSION_TOKEN]
   --quiet                               don't log errors to stderr
   --help, -h                            show help
   --version, -v                         print the version
```

## For localhost development
``` 
make install-schema-es
```

## For production

### Create the binaries
- Run `make temporal-elasticsearch-tool`
- You should see an executable `temporal-elasticsearch-tool`

### Schema setup
```
NAME:
   temporal-elasticsearch-tool setup-schema - setup elasticsearch cluster settings and index template

USAGE:
   temporal-elasticsearch-tool setup-schema [command options]

OPTIONS:
   --fail      fail silently on HTTP errors (default: false)
   --help, -h  show help
```

```
NAME:
   temporal-elasticsearch-tool create-index - create elasticsearch visibility index

USAGE:
   temporal-elasticsearch-tool create-index [command options]

OPTIONS:
   --index value  name of the visibility index to create
   --fail         fail silently on HTTP errors (default: false)
   --help, -h     show help
```

The tool now uses embedded schema files (cluster settings and index template) and provides separate commands for different operations. You can set up the schema and create indexes separately:

```
export ES_SERVER=http://127.0.0.1:9200
export ES_USER=$USER
export ES_PWD=$PWD

# Setup cluster settings and index template
temporal-elasticsearch-tool setup-schema

# Create the visibility index
temporal-elasticsearch-tool create-index --index temporal_visibility_v1

# Or combine both operations
temporal-elasticsearch-tool setup-schema && \
temporal-elasticsearch-tool create-index --index temporal_visibility_v1
```

### Ping
Ping the ES server to ensure connectivity and successful authentication

```
export ES_SERVER=http://127.0.0.1:9200
export ES_VERSION=v7
export AWS_REGION=us-east-1

temporal-elasticsearch-tool --aws environment ping 
```

### AWS Authentication
The CLI supports 3 AWS authentication mechanisms: `aws-sdk-default`, `environment`, `static`.

```
# aws-go-sdk defaults
export AWS_REGION=us-east-1
export ES_SERVER=http://127.0.0.1:9200

temporal-elasticsearch-tool --aws aws-sdk-default setup-schema
temporal-elasticsearch-tool --aws aws-sdk-default create-index --index temporal_visibility_v1
```

```
# Environment Credentials
export AWS_REGION=us-east-1
export ES_SERVER=http://127.0.0.1:9200

temporal-elasticsearch-tool --aws environment setup-schema
temporal-elasticsearch-tool --aws environment create-index --index temporal_visibility_v1
```

```
# Static Credentials
export AWS_REGION=us-east-1
export ES_SERVER=http://127.0.0.1:9200
export ES_USER=$AWS_ACCESS_KEY_ID
export ES_PWD=$AWS_SECRET_ACCESS_KEY

temporal-elasticsearch-tool --aws static setup-schema
temporal-elasticsearch-tool --aws static create-index --index temporal_visibility_v1
```

```
# Static w/ Session Token
export AWS_REGION=us-east-1
export ES_SERVER=http://127.0.0.1:9200
export ES_USER=$AWS_ACCESS_KEY_ID
export ES_PWD=$AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN

temporal-elasticsearch-tool --aws static setup-schema
temporal-elasticsearch-tool --aws static create-index --index temporal_visibility_v1
```

### Additional Commands

#### Upgrade Schema
Updates the index template to the latest version without affecting cluster settings or indexes:
```bash
temporal-elasticsearch-tool upgrade-schema
```

#### Drop Index
Deletes a visibility index:
```bash
temporal-elasticsearch-tool drop-index --index temporal_visibility_v1
```

#### Command Summary
- `setup-schema`: Sets up cluster settings and index template (no index creation)
- `upgrade-schema`: Updates index template only  
- `create-index`: Creates a new visibility index (requires --index flag)
- `drop-index`: Deletes a visibility index (requires --index flag)
- `ping`: Tests connectivity to Elasticsearch server
