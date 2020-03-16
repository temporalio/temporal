# Amazon S3 blobstore
## Configuration
See https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials on how to set up authentication against s3

Enabling archival is done by using the configuration below. `Region` and `bucket URI` are required
```
archival:
  history:
    status: "enabled"
    enableRead: true
    provider:
      s3store:
        region: "us-east-1"
  visibility:
    status: "enabled"
    enableRead: true
    provider:
      s3store:
        region: "us-east-1"

domainDefaults:
  archival:
    history:
      status: "enabled"
      URI: "s3://<bucket-name>"
    visibility:
      status: "enabled"
      URI: "s3://<bucket-name>"
```

## Visibility query syntax
You can query the visibility store by using the `cadence workflow listarchived` command

The syntax for the query is based on SQL

Supported column names are
- WorkflowID *String*
- WorkflowTypeName *String*
- StartTime *Date*
- CloseTime *Date*
- SearchPrecision *String - Day, Hour, Minute, Second*

WorkflowID or WorkflowTypeName is required. If filtering on date use StartTime or CloseTime in combination with SearchPrecision.

Searching for a record will be done in times in the UTC timezone

SearchPrecision specifies what range you want to search for records. If you use `SearchPrecision = 'Day'`
it will search all records starting from `2020-01-21T00:00:00Z` to `2020-01-21T59:59:59Z` 

### Limitations

- The only operator supported is `=` due to how records are stored in s3.

### Example

*Searches for all records done in day 2020-01-21 with the specified workflow id*

`./cadence --do samples-domain workflow listarchived -q "StartTime = '2020-01-21T00:00:00Z' AND WorkflowID='workflow-id' AND SearchPrecision='Day'"`
## Storage in S3
Workflow runs are stored in s3 using the following structure
```
s3://<bucket-name>/<domain-id>/
	history/<workflow-id>/<run-id>
	visibility/
            workflowTypeName/<workflow-type-name>/
                startTimeout/2020-01-21T16:16:11Z/<run-id>
                closeTimeout/2020-01-21T16:16:11Z/<run-id>
            workflowID/<workflow-id>/
                startTimeout/2020-01-21T16:16:11Z/<run-id>
                closeTimeout/2020-01-21T16:16:11Z/<run-id>
```

## Using localstack for local development
1. Install awscli from [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)
2. Install localstack from [here](https://github.com/localstack/localstack#installing)
3. Launch localstack with `SERVICES=s3 localstack start`
4. Create a bucket using `aws --endpoint-url=http://localhost:4572 s3 mb s3://cadence-development` 
5. Configure archival and domainDefaults with the following configuration
```
archival:
  history:
    status: "enabled"
    enableRead: true
    provider:
      s3store:
        region: "us-east-1"
        endpoint: "http://127.0.0.1:4572"
        s3ForcePathStyle: true
  visibility:
    status: "enabled"
    enableRead: true
    provider:
      s3store:
        region: "us-east-1"
        endpoint: "http://127.0.0.1:4572"
        s3ForcePathStyle: true

domainDefaults:
  archival:
    history:
      status: "enabled"
      URI: "s3://cadence-development"
    visibility:
      status: "enabled"
      URI: "s3://cadence-development"
```
