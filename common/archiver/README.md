## What

This README explains how to add new Archiver implementations.

There are two approaches:

1. **Built-in implementation** — add the archiver directly to this repository (e.g., `filestore`, `gcloud`, `s3store`). **We are not currently accepting contributions for new built-in archiver implementations.** Maintaining a growing set of built-in implementations places an ongoing maintenance burden on the team, so new implementations should use Option 2 instead.

2. **Custom implementation via server option** — implement the archiver in an external package and inject it into the server at startup using `WithCustomHistoryArchiverFactory` / `WithCustomVisibilityArchiverFactory`. This is the recommended approach for all new archiver implementations.

---

## Option 1: Built-in implementation (in-repo)

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
    // This method will be invoked after a workflow passes its retention period.
    // It's possible that this method will be invoked for one workflow multiple times and potentially concurrently,
    // implementation should correctly handle the workflow not exist case and return nil error.
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
    // Archive is used to archive one workflow visibility record.
    // Check the Archive() method of the HistoryArchiver interface in Step 2 for parameters' meaning and requirements.
    // The only difference is that the ArchiveOption parameter won't include an option for recording process.
    // Please make sure your implementation is lossless. If any in-memory batching mechanism is used, then those batched records will be lost during server restarts.
    // This method will be invoked when workflow closes. Note that because of conflict resolution, it is possible for a workflow to through the closing process multiple times, which means that this method can be invoked more than once after a workflow closes.
    Archive(context.Context, URI, *ArchiveVisibilityRequest, ...ArchiveOption) error

    // Query is used to retrieve archived visibility records.
    // Check the Get() method of the HistoryArchiver interface in Step 2 for parameters' meaning and requirements.
    // The request includes a string field called query, which describes what kind of visibility records should be returned. For example, it can be some SQL-like syntax query string.
    // Your implementation is responsible for parsing and validating the query, and also returning all visibility records that match the query.
    // Currently the maximum context timeout passed into the method is 3 minutes, so it's ok if this method takes a long time to run.
    Query(context.Context, URI, *QueryVisibilityRequest) (*QueryVisibilityResponse, error)

    // ValidateURI is used to define what a valid URI for an implementation is.
    ValidateURI(URI) error
}
```

**Step 4: Update provider to provide access to your implementation**

Modify the `./provider/provider.go` file so that the `ArchiverProvider` knows how to create an instance of your archiver.
Also, add configs for you archiver to static yaml config files and modify the `HistoryArchiverProvider`
and `VisibilityArchiverProvider` struct in the `../common/service/config.go` accordingly.

---

## Option 2: Custom implementation via server option (external package)

This approach lets you define archiver implementations in your own codebase and inject them into the Temporal server at startup, without modifying the server source.

**Step 1: Implement the HistoryArchiver and VisibilityArchiver interfaces**

Same interfaces as Steps 2 and 3 above.

**Step 2: Implement CustomHistoryArchiverFactory and/or CustomVisibilityArchiverFactory**

```go
// CustomHistoryArchiverFactory constructs a history archiver for a given URI scheme.
// Return provider.ErrUnknownScheme to fall back to the built-in implementation for that scheme.
// If a non-nil archiver is returned, it takes precedence over built-in archiver implementations.
type CustomHistoryArchiverFactory interface {
    NewCustomHistoryArchiver(provider.NewCustomHistoryArchiverParams) (archiver.HistoryArchiver, error)
}

// CustomVisibilityArchiverFactory constructs a visibility archiver for a given URI scheme.
// Return provider.ErrUnknownScheme to fall back to the built-in implementation for that scheme.
// If a non-nil archiver is returned, it takes precedence over built-in archiver implementations.
type CustomVisibilityArchiverFactory interface {
    NewCustomVisibilityArchiver(provider.NewCustomVisibilityArchiverParams) (archiver.VisibilityArchiver, error)
}
```

The params structs provide everything your factory needs to construct an archiver:

```go
type NewCustomHistoryArchiverParams struct {
    Scheme           string
    ExecutionManager persistence.ExecutionManager
    Logger           log.Logger
    MetricsHandler   metrics.Handler
    Configs          map[string]any  // from archival.history.provider.customStores.<scheme> in config yaml
}

type NewCustomVisibilityArchiverParams struct {
    Scheme         string
    Logger         log.Logger
    MetricsHandler metrics.Handler
    Configs        map[string]any  // from archival.visibility.provider.customStores.<scheme> in config yaml
}
```

Example factory implementation using the functional adapter types:

```go
historyFactory := provider.CustomHistoryArchiverFactoryFunc(func(params provider.NewCustomHistoryArchiverParams) (archiver.HistoryArchiver, error) {
    if params.Scheme != "myscheme" {
        return nil, provider.ErrUnknownScheme
    }
    return mypackage.NewHistoryArchiver(params.ExecutionManager, params.Logger, params.MetricsHandler, params.Configs)
})

visibilityFactory := provider.CustomVisibilityArchiverFactoryFunc(func(params provider.NewCustomVisibilityArchiverParams) (archiver.VisibilityArchiver, error) {
    if params.Scheme != "myscheme" {
        return nil, provider.ErrUnknownScheme
    }
    return mypackage.NewVisibilityArchiver(params.Logger, params.MetricsHandler, params.Configs)
})
```

**Step 3: Register the factories with the server**

Pass the factories as server options when constructing the Temporal server:

```go
s, err := temporal.NewServer(
    temporal.WithConfig(cfg),
    temporal.WithCustomHistoryArchiverFactory(historyFactory),
    temporal.WithCustomVisibilityArchiverFactory(visibilityFactory),
    // ... other options
)
```

**Step 4: Configure archival in your YAML config**

Enable archival and configure the URI scheme for your implementation. Use `customStores` to pass arbitrary config key-values to your factory:

```yaml
archival:
  history:
    state: "enabled"
    enableRead: true
    provider:
      customStores:
        myscheme:              # must match the scheme in your URIs
          endpoint: "https://my-storage.example.com"
          bucketName: "temporal-history"
  visibility:
    state: "enabled"
    enableRead: true
    provider:
      customStores:
        myscheme:
          endpoint: "https://my-storage.example.com"
          bucketName: "temporal-visibility"

namespaceDefaults:
  archival:
    history:
      state: "enabled"
      URI: "myscheme://temporal-history"
    visibility:
      state: "enabled"
      URI: "myscheme://temporal-visibility"
```

The `customStores.<scheme>` map is passed as `Configs` in the params to your factory. Built-in schemes (`filestore`, `gstorage`, `s3store`) continue to use their own config sections unless your factory handles them (see FAQ below).

---

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
    if featureCatalog.NonRetryableError != nil {
      return featureCatalog.NonRetryableError() // when the caller gets this error type back it will not retry anymore.
    }
  }
}
```

**How does my history archiver implementation read history?**

The `archiver` package provides a utility class called `HistoryIterator` which is a wrapper of `ExecutionManager`.
Its usage is simpler than `ExecutionManager`, so archiver implementations can choose to use it when reading workflow histories.
See the `historyIterator.go` file for more details.
Sample usage can be found in the filestore historyArchiver implementation.

**Can a custom factory override a built-in scheme like `filestore`?**

Yes. The custom factory is always consulted first. If your factory returns a non-nil archiver for a scheme that is also built-in (e.g., `filestore`), your implementation takes precedence and the built-in one is never used. Only return `ErrUnknownScheme` for schemes you want to delegate to the built-in implementations.

Note that when overriding a built-in scheme, the `Configs` field in the params is populated from `customStores.<scheme>` — not from the built-in config section (e.g., `filestore:`). If you need those config values, read them from `customStores` instead.

**Should my archiver define all its own error types?**

Each archiver is free to define and return any errors it wants. However many common errors which
exist between archivers are already defined in `constants.go`.

**Is there a generic query syntax for visibility archiver?**

Currently no. But this is something we plan to do in the future. As for now, try to make your syntax similar to the one used by our advanced list workflow API.
