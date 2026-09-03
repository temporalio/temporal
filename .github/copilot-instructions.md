# Code Review Guidelines

These rules apply when authoring or reviewing code; the Review Feedback section applies only when reviewing code.

## 1. Structural Simplicity (Highest Priority)

- Review changes holistically as well as line by line.
- Designs minimize branches, special cases, indirection, and moving parts.
- Every line contributes to the implementation or verifies behavior in a test.
- Tests contain only the activities and complexity needed to exercise the behavior in scope.
- Tests use deterministic inputs unless randomness is the behavior under test.
- Tests assert the behavior in scope and rely on unrelated operations to work. For example, a test that is not about `TerminateWorkflowExecution` assumes that operation works.
- Values assigned immediately before use are used without redundant nil checks.
- Packages export only identifiers required by their callers.

## 2. Go Conventions

- Getter names omit the `Get` prefix: use `func (a *Activity) Store()` rather than `GetStore()`.
- Implementation names omit the `Impl` suffix.
- Test names place their descriptive name directly after `Test`: use `TestRetry` rather than `Test_Retry`.
- Names omit package-name stuttering: package `activity` uses `Status` rather than `ActivityStatus`.
- Map lookups and other operations that return a presence boolean use the comma-ok pattern rather than nil checks.
- Defaults use `cmp.Or` when zero means "unset"; it returns the first non-zero argument. Side-effecting or expensive fallbacks use an `if` statement because all arguments are evaluated.

## 3. Testing Correctness and Reliability

- Subtests use their `t` parameter rather than `s.T()`.
- Testify suite assertion methods run in the test goroutine rather than worker goroutines.
- Eventually blocks containing assertions use `EventuallyWithT` and its block-local `t`.
- Specific error type assertions use `require.ErrorAs(t, err, &specificErr)`.
- Test assertions use `require` rather than `assert`, so a failed assertion stops the test before later checks run on an invalid state.
- Tests use a table-driven structure when multiple cases exercise the same behavior. Every case has a descriptive name and runs as a subtest.
- Independent cases in plain `t.Run` tests run in parallel. Testify suite subtests remain sequential because the suite does not support parallel subtests.
- Tests compare a function's complete result with an expected value rather than asserting each field separately. Proto results and proto fields within non-proto results use `protorequire.ProtoEqual`; field-level assertions are reserved for cases where only part of the result is relevant.
- Every use of `Eventually` has a comment explaining why polling is required, such as eventual consistency.
- Error type checks use a guarded API such as `errors.AsType`; single-value assertions such as `err.(*T)` are avoided because they panic when the type does not match.
- A goroutine that maintains a precondition for later assertions loops until context cancellation. This prevents a transiently failed attempt from exiting silently and leaving downstream eventual-consistency waits unable to succeed.
- Testify assertions run in the test goroutine. Worker goroutines return results or errors through buffered channels so an assertion cannot panic the binary after the test has completed.
- Blocking channel operations in tests use `await.Rcv` and `await.Snd` so they fail on timeout instead of hanging indefinitely.
- Package-level and global variables remain immutable during tests. Values are threaded through function parameters because parallel tests share the same process.
- Tests coordinate ordering with channels, `sync.WaitGroup`, or `EventuallyWithT` rather than `time.Sleep` or elapsed-time thresholds.
- A background operation that drives an `EventuallyWithT` condition has a longer timeout than the waiting deadline, so it remains capable of satisfying the condition for the entire wait.
- Errors from precondition operations are surfaced or retried until success when failure would invalidate the rest of the test; they are not discarded with `_, _ = f()`.
- A goroutine responsible for a transiently fallible precondition loops until `ctx.Done()` or reports success before the test waits for its effect; a single unverified attempt is insufficient.

## 4. Inline Code / Avoid Abstractions

- Strings used in only one place remain inline rather than becoming constants.
- Struct field values are assigned directly in their literals when possible.
- Types and generic structs provide behavior or meaning beyond the values they wrap.
- Small amounts of straightforward functionality are implemented directly rather than through a new dependency.
- Test-suite-level helpers are safe for use from subtests; helpers that depend on subtest-local state remain local to the subtest.
- Code uses explicit constructs rather than reflection.

## 5. Proper Error Handling

- Temporal errors use standard types such as `InvalidArgument`, `NotFound`, and `FailedPrecondition` rather than custom error types.
- Errors are non-retryable when their tasks must not retry in the queue.
- Wrapped errors add useful context, for example `fmt.Errorf("multi-operation part 2: %w", err)`; wrappers without additional information are omitted.
- Library code returns errors instead of panicking, leaving error handling to the caller.
- Handlers validate inputs at their boundaries rather than deep in business logic.
- Error type checks use `errors.AsType` rather than `errors.As`.
- Error-message assertions use `require.ErrorContains` rather than separate `require.Error` and `require.Contains` assertions.

## 6. Consistency with Codebase

- Nexus libraries pass through frontend requests consistently with the established pattern.
- Existing utilities are reused before new ones are created.
- Logger messages are static, and dynamic content is recorded in structured tags.
- CLI documentation follows the codebase's conventions, including capitalization of proper nouns.
- Metric tags match existing formats, including `CONSTANT_CASE` for enum values.
- Error messages match the surrounding code's style, including omission of punctuation for single sentences.

## 7. Code comments

- Comments are full sentences that start with a capital letter and end with punctuation. Short labels, end-of-line fragments, directives, and `TODO` markers are exempt.
- Comments explain behavior that is not apparent to a reader familiar with the codebase.
- Names convey information that can be expressed clearly through naming; comments cover information that naming cannot convey.
- Comments contain only the explanation needed to understand the code.
- Comments use literal language and terminology established in the codebase.
- Comments use simple sentence structures, with several plain statements rather than one sentence built from subordinate clauses, parentheticals, or stacked qualifications.
- Comments ordinarily describe the current code without referring to counterfactuals, prior discussions, or the decision process from when the code was written.
- Comments ordinarily describe the code itself rather than how upstream callers use it.
- Give concrete replacement text as a code suggestion. If restructuring the code is clearer and shorter than rewording the comment, suggest that code instead.

## 8. API and Proto Design

- Every proto field has a comment.
- Protobuf fields use canonical snake-case names such as `request_id` rather than `requestId`, and `schedule_time` rather than `scheduledTime`.
- User-facing errors describe user-facing concepts without exposing internal concepts such as `LowCardinalityKeyword`.
- Functions accept event-attribute structs instead of continually growing parameter lists.
- Well-known values use enums rather than integers or strings.

## 9. Concurrency and Safety

- Structs, especially proto messages, use immutable data patterns to avoid data races and synchronization.
- Synchronization uses `sync.Mutex` by default. Atomics are reserved for specific patterns or demonstrated performance concerns.
- Synchronization uses `sync.Mutex` rather than `sync.RWMutex` unless reads outnumber writes by more than 1000 to 1 or readers hold the lock for significant time.
- I/O occurs outside critical sections, with side-effect tasks carrying work that must happen after a lock is released.
- Data protected by a lock is cloned before the lock is released when it may be modified afterward.
- Proto message fields accessed outside the workflow lock are cloned with `common.CloneProto(...)` rather than aliased by returning their pointers directly.

## 10. Review Feedback

### Comment format

Use this core structure for every actionable finding.
Replace `SEVERITY` with `nit`, `small`, `med`, or `high`:

```markdown
<details>
<summary><strong>SEVERITY</strong> — One-line summary.</summary>

Concise explanation of what is wrong and why it matters, followed by any
supporting evidence, examples, or implementation notes.

</details>

**Suggestion:** Concrete fix or alternative.
```

Use HTML tags rather than Markdown inside `<summary>`.
The summary line is all a reader sees before expanding, so it must state the problem on its own.
Keep the suggestion outside the collapsible block, as a code suggestion wherever the fix is a concrete edit.

### Severity levels

- `nit` — Stylistic or trivial improvement. Preference-based. Non-blocking.
- `small` — Minor issue: slightly misleading name, small readability concern, or minor best-practice deviation. Does not affect correctness. Non-blocking.
- `med` — Moderate issue: missing error handling, logic that is likely wrong in edge cases, test gaps, or design concerns. Affects correctness or maintainability. Blocking.
- `high` — Serious issue: security vulnerability, data loss risk, crash/panic, race condition, broken functionality, or architectural violation. Blocking.

Report concrete findings at all four severity levels: `nit`, `small`, `med`, and `high`.
Prefer a small number of high-confidence findings.
Keep `nit` and `small` findings proportionally shorter than `med` and `high` findings.
Report concrete `nit` and `small` findings selectively, and consolidate related symptoms into a single comment that addresses the root issue.

### Feedback style

Be direct and practical, without fluff.
Reference specific codebase patterns and utilities, suggest concrete alternatives, and explain why something should change rather than only stating that it should.
