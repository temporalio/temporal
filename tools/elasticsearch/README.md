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
   temporal-elasticsearch-tool setup-schema - setup initial version of elasticsearch schema and index

USAGE:
   temporal-elasticsearch-tool setup-schema [command options] [arguments...]

OPTIONS:
   --settings-file value    path to the .json cluster settings file
   --template-file value    path to the .json index template file
   --index value, -i value  name of the visibility index to create
   --fail                   fail silently on HTTP errors
```

The options are optional. For example, if you only want to create the index you can provide just the `--index` argument. This allows you to choose which setup operations can fail by combining them with `--fail` to ignore errors.

```
export ES_SERVER=http://127.0.0.1:9200
export ES_USER=$USER
export ES_PWD=$PWD

# ESv6
ES_VERSION=v6 temporal-elasticsearch-tool setup \
    --settings-file ./schema/elasticsearch/visibility/cluster_settings_v6.json \
    --template-file ./schema/elasticsearch/visibility/index_template_v6.json \
    --index temporal_visibility_v1 

# ESv7
ES_VERSION=v7 temporal-elasticsearch-tool setup \
    --settings-file ./schema/elasticsearch/visibility/cluster_settings_v7.json \
    --template-file ./schema/elasticsearch/visibility/index_template_v7.json \
    --index temporal_visibility_v1 
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

temporal-elasticsearch-tool --aws aws-sdk-default setup \
    --settings-file ./schema/elasticsearch/visibility/cluster_settings_v6.json \
    --template-file ./schema/elasticsearch/visibility/index_template_v6.json \
    --index temporal_visibility_v1 
```

```
# Environment Credentials
export AWS_REGION=us-east-1
export ES_SERVER=http://127.0.0.1:9200

temporal-elasticsearch-tool --aws environment setup \
    --settings-file ./schema/elasticsearch/visibility/cluster_settings_v6.json \
    --template-file ./schema/elasticsearch/visibility/index_template_v6.json \
    --index temporal_visibility_v1 
```

```
# Static Credentials
export AWS_REGION=us-east-1
export ES_SERVER=http://127.0.0.1:9200
export ES_USER=$AWS_ACCESS_KEY_ID
export ES_PWD=$AWS_SECRET_ACCESS_KEY

temporal-elasticsearch-tool --aws static setup \
    --settings-file ./schema/elasticsearch/visibility/cluster_settings_v6.json \
    --template-file ./schema/elasticsearch/visibility/index_template_v6.json \
    --index temporal_visibility_v1 
```

```
# Static w/ Session Token
export AWS_REGION=us-east-1
export ES_SERVER=http://127.0.0.1:9200
export ES_USER=$AWS_ACCESS_KEY_ID
export ES_PWD=$AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN

temporal-elasticsearch-tool --aws static setup \
    --settings-file ./schema/elasticsearch/visibility/cluster_settings_v6.json \
    --template-file ./schema/elasticsearch/visibility/index_template_v6.json \
    --index temporal_visibility_v1 
```
