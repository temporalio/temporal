The `temporaltest` package provides helpers for writing end to end tests against a real Temporal server which can be run via the `go test` command.

## Backwards Compatibility

This package must not break Go API backwards compatibility in accordance with semantic versioning. One exception to this policy is the `WithBaseServerOptions` function, which may have breaking changes in any Temporal server release.

The base configuration (eg. dynamic config values) and behavior of `TestServer` may also be modified in any release. Such changes should be for the purposes of improving performance or stability for testing scenarios.
