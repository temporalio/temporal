# LRU Test Lint Cleanup Design

## Scope

Fix all 236 golangci-lint findings in `common/cache/lru_test.go` without changing production code or existing comments.

## Changes

- Replace package-level Testify `assert` calls with fatal `require` assertions.
- Apply the specific Testify helpers requested by `testifylint`, including `Len`, `Nil`, `NoError`, `InDelta`, and ordered expected/actual arguments.
- Replace the six `require.Eventually` calls with `await` helpers while preserving their timeouts and polling intervals.
- Use `await.Require` when a retry advances the event time source or needs assertion diagnostics; use `await.RequireTrue` for a simple channel-completion predicate.
- Replace the flagged `WaitGroup` Add/Done goroutine pattern with `WaitGroup.Go`.
- Update imports only as required by these changes.

## Behavioral Constraints

- Preserve every existing comment.
- Preserve cache operations, test inputs, retry deadlines, and polling intervals.
- Keep the change isolated to `common/cache/lru_test.go` apart from this design document.
- Strengthening assertions from non-fatal to fatal is intentional and follows the repository's Go test guidance.

## Verification

1. Format the changed Go file using the repository tooling.
2. Run the cache package tests with the required `test_dep` build tag.
3. Run golangci-lint against the cache package and confirm that `common/cache/lru_test.go` has no findings.
4. Run `make lint-code` before opening the draft pull request.

## Delivery

Commit the focused change, push the branch, and open a draft pull request with the verification results in its description.
