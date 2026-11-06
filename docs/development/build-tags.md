# Build tags

These Go build tags compile out parts of the server. For tags that only affect tests, see
[Testing](testing.md#build-tags).

## `disable_grpc_modules`

Excludes the gRPC dependencies of `cloud.google.com/go/storage`, reducing binary size and compile
time. The server only uses the storage REST client (`storage.NewClient`), not the gRPC client
(`storage.NewGRPCClient`).

The Makefile and release builds (`.goreleaser.yml`) set it by default.

## Optional integrations

By default the server is built with every integration it supports. Programs that don't need an
integration can compile it out with the `temporal_minimal` build tag, and opt back in to individual
integrations with `temporal_with_<integration>` tags:

```sh
# Without optional integrations.
make temporal-server BUILD_TAG=temporal_minimal

# Without optional integrations, except Google Cloud Storage archival.
make temporal-server BUILD_TAG=temporal_minimal,temporal_with_gstorage

# Unit tests of the packages with a temporal_minimal variant.
make unit-test-build-tags
```

Without `temporal_minimal`, the `temporal_with_*` tags have no effect and every integration is
included.

| Tag                               | Integration                             | Dependencies it brings in                         |
| --------------------------------- | --------------------------------------- | ------------------------------------------------- |
| `temporal_with_elasticsearch`     | Elasticsearch visibility                | Elasticsearch client                              |
| `temporal_with_gstorage`          | Google Cloud Storage archival           | Google Cloud Storage SDK                          |
| `temporal_with_s3store`           | S3 archival                             | AWS SDK S3 client                                 |
| `temporal_with_worker_controller` | Worker controller and compute providers | AWS Lambda, ECS, AgentCore and GCP Cloud Run SDKs |

A server built without an archival or visibility integration refuses to start if its configuration
sets the integration up.

Without `temporal_with_worker_controller`, a minimal server rejects compute configuration creation,
updates and validation with `FailedPrecondition` identifying the required build tag. Worker Deployment
versioning without compute configuration remains available. Describing a version with persisted
compute configuration or a running Worker Controller Instance also returns this error, without
querying a workflow that has no worker.

Use the same worker controller build tags across server services. Keep `workercontroller.enabled`
disabled in every namespace, and do not run the excluded worker service against persisted worker
controller or compute validation workflows. The excluded worker service refuses to start when the
global setting is enabled. Startup does not inspect all namespace overrides or persisted workflows.
Namespace overrides and dynamic configuration updates cannot enable the compiled out feature;
compute requests are rejected even when this setting is enabled. To operate existing compute
deployments, rebuild with `temporal_with_worker_controller`.

Kubernetes compute providers already require `computeprovider_k8s && !release`; this exclusion does
not change that requirement.

`make build-tags-check`, which runs in CI, checks the packages each `temporal_with_*` tag compiles out
of the `temporal_minimal` server, as listed in the `EXCLUDED_PACKAGES_<tag>` variables in the
`Makefile`. When adding an integration, add a variable for its tag.
