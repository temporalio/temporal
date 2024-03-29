# Amazon S3 blobstore
## Configuration
See https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials on how to set up authentication against s3

Enabling archival is done by using the configuration below. `Region` and `bucket URI` are required
```
archival:
  history:
    state: "enabled"
    enableRead: true
    provider:
      s3store:
        region: "us-east-1"
        logLevel: 0
  visibility:
    state: "enabled"
    enableRead: true
    provider:
      s3store:
        region: "us-east-1"
        logLevel: 0

namespaceDefaults:
  archival:
    history:
      state: "enabled"
      URI: "s3://<bucket-name>"
    visibility:
      state: "enabled"
      URI: "s3://<bucket-name>"
```

## Visibility query syntax
You can query the visibility store by using the `tctl workflow listarchived` command

The syntax for the query is based on SQL

Supported column names are
- WorkflowId *String*
- WorkflowTypeName *String*
- StartTime *Date*
- CloseTime *Date*
- SearchPrecision *String - Day, Hour, Minute, Second*

WorkflowId or WorkflowTypeName is required. If filtering on date use StartTime or CloseTime in combination with SearchPrecision.

Searching for a record will be done in times in the UTC timezone

SearchPrecision specifies what range you want to search for records. If you use `SearchPrecision = 'Day'`
it will search all records starting from `2020-01-21T00:00:00Z` to `2020-01-21T59:59:59Z` 

### Limitations

- The only operator supported is `=` due to how records are stored in s3.

### Example

*Searches for all records done in day 2020-01-21 with the specified workflow id*

`./tctl --ns samples-namespace workflow listarchived -q "StartTime = '2020-01-21T00:00:00Z' AND WorkflowId='workflow-id' AND SearchPrecision='Day'"`
## Storage in S3
Workflow runs are stored in s3 using the following structure
```
s3://<bucket-name>/<namespace-id>/
	history/<workflow-id>/<run-id>
	visibility/
            workflowTypeName/<workflow-type-name>/
                startTimeout/2020-01-21T16:16:11Z/<run-id>
                closeTimeout/2020-01-21T16:16:11Z/<run-id>
            workflowID/<workflow-id>/
                startTimeout/2020-01-21T16:16:11Z/<run-id>
                closeTimeout/2020-01-21T16:16:11Z/<run-id>
```

Enable AWS SDK Logging with config parameter `logLevel`. For example enable debug logging with `logLevel: 4096`. Possbile Values:
* LogOff = 0 = 0x0
* LogDebug = 4096 = 0x1000
* LogDebugWithSigning = 4097 = 0x1001
* LogDebugWithHTTPBody = 4098 = 0x1002
* LogDebugWithRequestRetries = 4100 = 0x1004
* LogDebugWithRequestErrors = 4104 = 0x1008
* LogDebugWithEventStreamBody = 4112 = 0x1010
* LogDebugWithDeprecated = 4128 = 0x1020


## Permissions

Your s3 user must have at least the following permissions:

* s3:ListBucket
* s3:GetObject
* s3:PutObject

## Using localstack for local development
1. Install awscli from [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
2. Install localstack from [here](https://github.com/localstack/localstack#installing)
3. Launch localstack with `SERVICES=s3 localstack start`
4. Create a bucket using `aws --endpoint-url=http://localhost:4566 s3 mb s3://temporal-development` 
5. Launch the server with the localstack s3 environment config`--env development-cass-s3 start`
