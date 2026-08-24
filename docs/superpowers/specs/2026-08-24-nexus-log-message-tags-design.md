# Nexus Log Message Tags

## Goal

Make three Nexus log messages aggregatable by keeping caller-controlled values out of the message and recording those values as structured tags.

## Production changes

- In `taskQueuePartitionManagerImpl.DispatchNexusTask`, replace the formatted invalid `Operation-Timeout` warning with a static message and tag the raw header value.
- In `NexusEndpointClient.List`, replace the formatted persistence error with a static message and tag the next-page token and page size.
- In `NexusEndpointClient.listAndFilterByName`, replace the formatted persistence error with a static message and tag the current persistence page token, page size, and endpoint name.
- Use typed logging tags directly at each call site. A shared abstraction would add indirection without consolidating meaningful behavior.

The parsing and error behavior remain unchanged. Only the structure of the emitted log records changes.

## Verification

Stack the change on PR #11689 so its functional-test log capture is available.

- Extend the Nexus matching functional flow with an invalid caller-controlled `Operation-Timeout` header. Assert that the warning has a static message and retains the raw value as a tag.
- Add focused frontend unit tests with mocked persistence failures. Assert the static messages and request-context tags for both unfiltered and name-filtered endpoint listing.
- Follow test-driven development by running each regression test against the existing implementation and confirming that it fails because the message is still dynamic or the expected tags are absent.
- Run the focused Go tests with `-tags test_dep`, format imports, and run `make lint-code` before creating the draft PR.

## Trade-offs and failure modes

The functional test covers the highest-risk path from caller input into Matching while avoiding a dedicated-cluster persistence fault-injection test. The endpoint-client unit tests are faster and isolate the exact persistence-error branches.

There is no runtime control-flow, retry, or persistence change. Under increased load, message cardinality remains bounded by the three static failure classes; tag values continue to preserve diagnostic context.
