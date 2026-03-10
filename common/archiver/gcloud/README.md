# Google Storage blobstore
## Configuration
See https://cloud.google.com/docs/authentication#service-accounts to understand how is made the authentication against google cloud storage

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
You can query the visibility store by using the `temporal workflow list --archived` command

The syntax for the query is based on SQL

Supported column names are
- `WorkflowId` *String*
- `WorkflowType` *String*
- `RunId` *String*
- `StartTime` *Date*
- `CloseTime` *Date*
- `SearchPrecision` *String - Day, Hour, Minute, Second*

There are two types of queries:
- WorkflowId based queries
- Time based queries

### WorkflowId based queries

You can query for a set of workflows by providing the WorkflowId. Since a workflow ID does not have to be unique, the query can return  multiple workflow history records.

If you run a WorkflowId based query, you can also add the following fields to the query to reduce the number of archived workflow history recrords returned:
- `CloseTime`
- `SearchPrecision` (must be used with `StartTime`)
- `WorkflowType`
- `RunId`

*Simple Example:*

```
./temporal workflow list -n samples-namespace --archived --page-size=20 -q "WorkflowID = 'workflow-id'"
```

*_IMPORTANT_*: If you need to reduce the results using a workflowID based query as suggested above, please note that you have to provide a time based query first before you can use WorkflowType and if you want to use RunId, you have to specify a time based query and WorkflowType.

*Examples:*

Valid query:
```
./temporal workflow list -n samples-namespace --archived --page-size=20 -q "WorkflowID = 'workflow-id' AND StartTime = '2020-01-21T00:00:00Z' AND SearchPrecision='Day' AND WorkflowType = 'workflow-type' AND RunId = 'run-id'"
```

Queries that may return with zero results (although there may be archived workflows matching the criteria):
```
./temporal workflow list -n samples-namespace --archived --page-size=20 -q "WorkflowID = 'workflow-id' AND StartTime = '2020-01-21T00:00:00Z' AND SearchPrecision='Day' AND RunId = 'run-id'"
```
or
```
./temporal workflow list -n samples-namespace --archived --page-size=20 -q "WorkflowID = 'workflow-id' AND WorkflowType = 'workflow-type' AND RunId = 'run-id'"`
```

### Time based queries

To perform a time based query, one of the following fields are required: `StartTime` or `CloseTime` and they are *mutually exclusive*. In addition, `SearchPrecision` is also required.

`StartTime` or `CloseTime` must be submitted as Date-Time string using RFC3339 format, i.e. `yyyy-mm-ddTHH:MM:SS.msZ` with
- `yyyy` as the four digit year (e.g. `2026`)
- `mm` as two digit month (e.g. `05` for May)
- `dd` as two digit day of the month (e.g. `07`)
- `T` as separator between date and time
- `HH` as two digit hours using the 24-hour clock (e.g. `07` for 7am or `17` for 5pm)
- `MM` as two digit minutes (`00` to `59`)
- `SS` as two digits seconds (`00` to `59`)
- `ms` as two digits milliseconds (`00` to `99`)
- `Z` indicating the Zuu timezone

SearchPrecision specifies what range you want to search for records. If you use `SearchPrecision = 'Day'`
it, for example, will search all records starting from `2020-01-21T00:00:00Z` to `2020-01-21T59:59:59Z` 

To further limit the results, you can add the following fields to the query using the AND clause:
- `WorkflowType`
- `WorkflowId`
- `RunId`

Similar to the WorflowId based queries, if you want to filter by `RunId` you will have to provide a `WorkflowType` *and* a `WorkflowId` and if you want to just filter by `WorkflowId` you have to provide a `WorkflowType`. More on this is explained below in the limitations.

### Limitations

#### *_IMPORTANT_* limitation on filtering by specific columns:

With the gcloud archiver, visibility and history records are stored as generic objects into a specified bucket. In Google Cloud Storage, an object is identified by a unique name inside the bucket. There is no hierarchy, such as folders. Although the Google Cloud Console may visualize a "folder" structure, these are technically not folders. The console simply interprets a name that uses a forward slash (`/`) in its name as folder delimiter.
To support the documented search query, the gcloud archiver is creating three different index files in the visibility archive:

1. for `StartTime` search,
2. for `CloseTime` search, and
3. to support `WorkflowId` search.

The object naming for

#1 is `<namespace-id>/startTimeout_yyyy-mm-ddTHH-MM-ss-msZ_hash(WorkflowType)_hash(WorkflowId)_hash(RunId).visibility`,

#2 is `<namespace-id>/closeTimeout_yyyy-mm-ddTHH-MM-ss-msZ_hash(WorkflowType)_hash(WorkflowId)_hash(RunId).visibility`, and

#3 is `<namespace-id>/workflowID_hash(WorkflowId)_yyyy-mm-ddTHH-MM-ss-msZ_hash(WorkflowType)_hash(RunId).visibility`

To search for a specific object, the gcloud API provides a prefix-search. That means that we can "search" for objects whose name starts with a given prefix similar to a wildcard search on a file system using `filename*`. This results in the requirement to provide certain query fields in order to filter on others that are placed further towards the end of the object name.

#### Other Limitations

- The only operator supported is `=`
- Currently It's not possible to guarantee the resulSet order, specially if the pageSize it's fullfilled.  

### Example

*Searches the first 20 records for a given day 2020-01-21*

```
./temporal workflow list -n samples-namespace --archived --page-size=20 -q "StartTime = '2020-01-21T00:00:00Z' AND SearchPrecision='Day'"
```



## History query syntax

Once you have a `workflowId` and a `runId` you can retrieve your workflow history record.

example:

```
./temporal workflow show -n samples-namespace -w workflow-id -r run-id
```
This will retrieve the workflow history either from the history store before expiration of the retention period or
from the archive after the expired records have been removed from the history store.

## General

For more details on the `temporal` command, please run

```
temporal --help
```

and for details about each command

```
temporal [command] --help
```