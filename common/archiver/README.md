## What

This README explains how to add new Archiver implementations.

## Steps

**Step 1: Create a new package for your implementation**

Create a new directory in the `archiver` folder. The structure should look like the following:
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

**Step 2: Implement the HistoryArchiver interface**

```go
type HistoryArchiver interface {
	// Archive is used to archive a workflow history. When the context expires the method should stop trying to archive.
	// Implementors are free to archive however they want, including implementing retries of sub-operations. The URI defines
	// the resource that histories should be archived into. The implementor gets to determine how to interpret the URI.
	// The Archive method may or may not be automatically retried by the caller. The ArchiveOptions are used
	// to interact with these retries including giving the implementor the ability to cancel retries and record progress
	// between retry attempts. 
    Archive(context.Context, URI, *ArchiveHistoryRequest, ...ArchiveOption) error
    
    // Get is used to access an archived history. When context expires method should stop trying to fetch history.
    // The URI identifies the resource from which history should be accessed and it is up to the implementor to interpret this URI.
    // This method should thrift errors - see filestore as an example.
    Get(context.Context, URI, *GetHistoryRequest) (*GetHistoryResponse, error)
    
    // ValidateURI is used to define what a valid URI for an implementation is.
    ValidateURI(URI) error
}
```

**Step 3: Implement the VisibilityArchiver interface**

```go
type VisibilityArchiver interface {
    // ValidateURI is used to define what a valid URI for an implementation is.
    ValidateURI(URI) error
}
```
VisibilityArchiver is plumbed throughout the codebase but the full visibility feature is not yet implemented.
This means even once the VisibilityArchiver is implemented archival of visibility events will not occur. 
Despite this implementors are still expected to implement this one method interface.

**Step 4: Update provider to provide access to your implementation**

Modify the `./provider/provider.go` file so that the `ArchiverProvider` knows how to create an instance of your archiver. 
Also, add configs for you archiver to static yaml config files and modify the `HistoryArchiverProvider` 
and `VisibilityArchiverProvider` struct in the `../common/service/config.go` accordingly.


## FAQ
**If my Archive method can automatically be retried by caller how can I record and access progress between retries?**

ArchiverOptions is used to handle this. The following shows and example: 
```go
func (a *Archiver) Archive(
	ctx context.Context,
	URI string,
	request *ArchiveRequest,
	opts ...ArchiveOption,
) error {
  featureCatalog := GetFeatureCatalog(opts...) // this function is defined in options.go

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

**If my Archive method encounters an error which is non-retryable how do I indicate that the caller should not retry?**

```go
func (a *Archiver) Archive(
	ctx context.Context,
	URI string,
	request *ArchiveRequest,
	opts ...ArchiveOption,
) error {
  featureCatalog := GetFeatureCatalog(opts...) // this function is defined in options.go

  err := youArchiverImpl()
  if nonRetryableErr(err) {
    if featureCatalog.NonRetriableError != nil {
	  return featureCatalog.NonRetriableError() // when the caller gets this error type back it will not retry anymore.
    }
  }
}
```

**How does my archiver implementation read history?**

The `archiver` package provides a utility class called `HistoryIterator` which is a wrapper of `HistoryManager`. 
Its usage is simpler than the `HistoryManager` given in the `BootstrapContainer`, 
so archiver implementations can choose to use it when reading workflow histories. 
See the `historyIterator.go` file for more details. 
Sample usage can be found in the filestore historyArchiver implementation.

**Should my archiver define all its own error types?**

Each archiver is free to define and return any errors it wants. However many common errors which
exist between archivers are already defined in `constants.go`.