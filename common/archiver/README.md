## What

This README explains how to create new implementations for the History and Visibility Archiver Interface.

## Steps

1. Create a new directory in the `archiver` folder. The structure should look like the following:

```
./common/archiver
  - filestore/                      -- Filestore implementation 
  - provider/
      - provider.go                 -- Provider of archiver instances
  - yourImplementation/
      - historyArchiver.go          -- HistoryArchiver implementation
      - historyArchiver_test.go     -- Unit tests for HistoryArchiver
      - visibilityArchiver.go       -- VisibilityArchiver implementations
      - visibilityArchiver_test.go  -- Unit tests for VisibilityArchiver
```
2. Each Archiver will be given a `BootstrapContainer` when it's created and the archiver implementation can (will) use them to read history, emit logs/metrics etc. See `interface.go` for more details.

3. Implement the Archiver interface. Both Archiver interfaces contain three methods:
  
```go
func Archive(ctx context.Context, URI string, request *ArchiveRequest, opts ...ArchiveOption) error

func Get(ctx context.Context, URI string, request *GetRequest) (*GetResponse, error)

func ValidateURI(URI string) error
```

Each archiver implementation should define a format of URI, which is used to describe the type of implementation and the target location of the `Archive()` or `Get()` operation. For example, the URI for filestore uses the format: `file:///path/to/dir`. The `ValidateURI()` method is responsible for checking if a given string is a valid URI.

The `Archive()` method should archive workflow histories or visibility records described by the archive request to the location specified by the URI, while the `Get()` method is responsible for retrieving those archived data.

The `Archive()` method may be invoked differently by different callers. For example some callers may automatically retry, while others only try once. Therefore, a list of `ArchiveOption` will be passed to this method. These options will be applied to an `ArchiveFeatureCatalog`, and by checking the fields of that catalog, the `Archive()` method can figure out what to do. Right now, the only feature in the catalog is `ProgressManager` which can be used to record and load archive progress. More features can be added if needed. The correct way to use it is shown in the code sample below.

```go
func (a *Archiver) Archive(
	ctx context.Context,
	URI string,
	request *ArchiveRequest,
	opts ...ArchiveOption,
) error {
  featureCatalog := **GetFeatureCatalog**(opts...) // this function is defined in options.go

  var progress progress

  // Check if the feature for recording progress is enabled.
  if featureCatalog.ProgressManager != nil {
    if err := featureCatalog.ProgressManager.LoadProgress(ctx, &prevProgress); err != nil {
      // log some error message and return error if needed.
    }
  }

  // Your archiver implementation...

  // Record current progress
  if featureCatalog.ProgressManager != nil {
    if err := featureCatalog.ProgressManager.RecordProgress(ctx, progress); err != nil {
      // log some error message and return error if needed. 
    }
  }
}
```

4. The `archiver` package provides a utility class called `HistoryIterator` which is a wrapper of `HistoryManager`. Its usage is simpler than the `HistoryManager` given in the `BootstrapContainer`, so archiver implementations can choose to use it when reading workflow histories. See the `historyIterator.go` file for more details. Sample usage can be found in the filestore historyArchiver implementation.

5. The `archiver` package also provides some common error types and messages in `constants.go`.

6. After you have finished the implementation, modify the `./provider/provider.go` file so that the `ArchiverProvider` knows how to create an instance of your archiver. Also, add configs for you archiver to static yaml config files and modify the `HistoryArchiverConfig` and `VisibilityArchiverConfig` struct in the `../common/service/config.go` accordingly.
