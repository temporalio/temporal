## Using the Elasticsearch schema tool

⚠️ **EXPERIMENTAL**: This tool is experimental and may change in future versions.

This package contains the tooling for temporal elasticsearch operations. 

### Usage
```
NAME:
   temporal-elasticsearch-tool - Command line tool for temporal elasticsearch operations (EXPERIMENTAL)

USAGE:
   temporal-elasticsearch-tool [global options] command [command options] [arguments...]

VERSION:
   0.0.1

COMMANDS:
   setup-schema    setup elasticsearch cluster settings and index template
   update-schema   update elasticsearch index template, or both template and index mappings if --index is specified
   create-index    create elasticsearch visibility index
   drop-index      delete elasticsearch visibility index
   ping            pings the elasticsearch host
   help, h         Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --endpoint value                    hostname or ip address of elasticsearch server (default: "http://127.0.0.1:9200") [$ES_SERVER]
   --user value                        username for elasticsearch or aws_access_key_id if using static aws credentials [$ES_USER]
   --password value                    password for elasticsearch or aws_secret_access_key if using static aws credentials [$ES_PWD]
   --aws-credentials value             AWS credentials provider (supported ['static', 'environment', 'aws-sdk-default']) [$AWS_CREDENTIALS]
   --aws-session-token value           AWS sessiontoken for use with 'static' AWS credentials provider [$AWS_SESSION_TOKEN]
   --tls                               enable TLS for elasticsearch connection [$ES_TLS]
   --tls-cert-file value               path to TLS certificate file (tls must be enabled) [$ES_TLS_CERT_FILE]
   --tls-key-file value                path to TLS key file (tls must be enabled) [$ES_TLS_KEY_FILE]
   --tls-ca-file value                 path to TLS CA certificate file (tls must be enabled) [$ES_TLS_CA_FILE]
   --tls-server-name value             TLS server name for host name verification (tls must be enabled) [$ES_TLS_SERVER_NAME]
   --tls-disable-host-verification     disable TLS host name verification (tls must be enabled) [$ES_TLS_DISABLE_HOST_VERIFICATION]
   --index value                       name of the visibility index [$ES_VISIBILITY_INDEX]
   --quiet                             don't log errors to stderr (default: false)
   --help, -h                          show help
   --version, -v                       print the version
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
export ES_VISIBILITY_INDEX=temporal_visibility_v1

# Setup cluster settings and index template
temporal-elasticsearch-tool setup-schema

# Create the visibility index (uses ES_VISIBILITY_INDEX environment variable)
temporal-elasticsearch-tool create-index

# Or combine both operations
temporal-elasticsearch-tool setup-schema && \
temporal-elasticsearch-tool create-index
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
export ES_VISIBILITY_INDEX=temporal_visibility_v1

temporal-elasticsearch-tool --aws aws-sdk-default setup-schema
temporal-elasticsearch-tool --aws aws-sdk-default create-index
```

```
# Environment Credentials
export AWS_REGION=us-east-1
export ES_SERVER=http://127.0.0.1:9200
export ES_VISIBILITY_INDEX=temporal_visibility_v1

temporal-elasticsearch-tool --aws environment setup-schema
temporal-elasticsearch-tool --aws environment create-index
```

```
# Static Credentials
export AWS_REGION=us-east-1
export ES_SERVER=http://127.0.0.1:9200
export ES_USER=$AWS_ACCESS_KEY_ID
export ES_PWD=$AWS_SECRET_ACCESS_KEY
export ES_VISIBILITY_INDEX=temporal_visibility_v1

temporal-elasticsearch-tool --aws static setup-schema
temporal-elasticsearch-tool --aws static create-index
```

```
# Static w/ Session Token
export AWS_REGION=us-east-1
export ES_SERVER=http://127.0.0.1:9200
export ES_USER=$AWS_ACCESS_KEY_ID
export ES_PWD=$AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN
export ES_VISIBILITY_INDEX=temporal_visibility_v1

temporal-elasticsearch-tool --aws static setup-schema
temporal-elasticsearch-tool --aws static create-index
```

### TLS Configuration
The tool supports TLS for secure connections to Elasticsearch.

#### Basic TLS with CA certificate
```bash
export ES_SERVER=https://elasticsearch.example.com:9200
export ES_TLS=true
export ES_TLS_CA_FILE=/path/to/ca.crt
export ES_USER=elastic
export ES_PWD=password

temporal-elasticsearch-tool setup-schema
temporal-elasticsearch-tool create-index
```

#### TLS with client certificate authentication
```bash
export ES_SERVER=https://elasticsearch.example.com:9200
export ES_TLS=true
export ES_TLS_CA_FILE=/path/to/ca.crt
export ES_TLS_CERT_FILE=/path/to/client.crt
export ES_TLS_KEY_FILE=/path/to/client.key
export ES_USER=elastic
export ES_PWD=password

temporal-elasticsearch-tool setup-schema
temporal-elasticsearch-tool create-index
```

#### TLS with custom server name
```bash
export ES_SERVER=https://elasticsearch.example.com:9200
export ES_TLS=true
export ES_TLS_CA_FILE=/path/to/ca.crt
export ES_TLS_SERVER_NAME=elasticsearch.internal
export ES_USER=elastic
export ES_PWD=password

temporal-elasticsearch-tool setup-schema
```

#### TLS with disabled host verification (not recommended for production)
```bash
export ES_SERVER=https://elasticsearch.example.com:9200
export ES_TLS=true
export ES_TLS_DISABLE_HOST_VERIFICATION=true
export ES_USER=elastic
export ES_PWD=password

temporal-elasticsearch-tool setup-schema
```

#### Using command line flags
All TLS options can also be specified as command line flags:

```bash
temporal-elasticsearch-tool \
  --endpoint https://elasticsearch.example.com:9200 \
  --tls \
  --tls-ca-file /path/to/ca.crt \
  --tls-cert-file /path/to/client.crt \
  --tls-key-file /path/to/client.key \
  --tls-server-name elasticsearch.internal \
  --user elastic \
  --password password \
  setup-schema
```

### Additional Commands

#### Update Schema
Updates the index template to the latest version, or updates both the template and index mappings if `--index` is specified:

Update template only:
```bash
temporal-elasticsearch-tool update-schema
```

Update both template and index mappings:
```bash
temporal-elasticsearch-tool update-schema --index temporal_visibility_v1
```

Update both template and index mappings using environment variable:
```bash
export ES_VISIBILITY_INDEX=temporal_visibility_v1
temporal-elasticsearch-tool update-schema
```

#### Drop Index
Deletes a visibility index:
```bash
temporal-elasticsearch-tool drop-index --index temporal_visibility_v1
```

Or using environment variable:
```bash
export ES_VISIBILITY_INDEX=temporal_visibility_v1
temporal-elasticsearch-tool drop-index
```

#### Command Summary
- `setup-schema`: Sets up cluster settings and index template (no index creation)
- `update-schema`: Updates index template, or both template and index mappings if --index is specified
- `create-index`: Creates a new visibility index (requires --index flag)
- `drop-index`: Deletes a visibility index (requires --index flag)
- `ping`: Tests connectivity to Elasticsearch server
