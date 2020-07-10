# Google Storage blobstore
## Configuration
See https://cloud.google.com/docs/authentication/production to understand how is made the authentication against google cloud storage

Nowdays we support three different ways in order to let Temporal know where your google keyfile credentials are located

* Temporal archival deployment.yaml configuration file
* `GOOGLE_APPLICATION_CREDENTIALS` environment variable
*  Google default credentials location

If more than one credentials location is given, then Temporal will resolve the conflicts by the following priority:

`GOOGLE_APPLICATION_CREDENTIALS > Temporal archival deployment.yaml > Google default credentials`

Be sure that you have created your bucket first, and have enought rights in order to read/write over your bucket.

### Gcloud Archival example

Enabling archival is done by using the configuration below. `credentialsPath` is required but could be empty "" in order to allow a Google default credentials.

```
archival:
  history:
    state: "enabled"
    enableRead: true
    provider:
      gstorage:
        credentialsPath: "/tmp/keyfile.json"
  visibility:
    state: "enabled"
    enableRead: true
    provider:
      gstorage:
        credentialsPath: "/tmp/keyfile.json"

namespaceDefaults:
  archival:
    history:
      state: "enabled"
      URI: "gs://my-bucket-cad/temporal_archival/development"
    visibility:
      state: "enabled"
      URI: "gs://my-bucket-cad/temporal_archival/visibility"
```

## Visibility query syntax
You can query the visibility store by using the `tctl workflow listarchived` command

The syntax for the query is based on SQL

Supported column names are
- WorkflowType *String*
- WorkflowID *String*
- StartTime *Date*
- CloseTime *Date*
- SearchPrecision *String - Day, Hour, Minute, Second*

One of these fields are required, StartTime or CloseTime and they are mutually exclusive and also SearchPrecision.

Searching for a record will be done in times in the UTC timezone

SearchPrecision specifies what range you want to search for records. If you use `SearchPrecision = 'Day'`
it will search all records starting from `2020-01-21T00:00:00Z` to `2020-01-21T59:59:59Z` 

### Limitations

- The only operator supported is `=`
- Currently It's not possible to guarantee the resulSet order, specially if the pageSize it's fullfilled.  

### Example

*Searches the first 20 records for a given day 2020-01-21*

`./tctl --ns samples-namespace workflow listarchived -ps="20" -q "StartTime = '2020-01-21T00:00:00Z' AND SearchPrecision='Day'"`

## Archival query syntax

Once you have a workflowId and a runId you can retrieve your workflow history.

example:

`./tctl --ns samples-namespace  workflow  show  -w workflow-id -r runId`