# Code Review Guidelines

Apply these patterns when reviewing PRs or suggesting code changes.

## 1. Structural Simplicity (Highest Priority)

- Review changes holistically as well as line by line
- Prefer simpler designs that remove branches, special cases, indirection, or moving parts
- Remove code that doesn't add value to tests or implementation
- Don't add unnecessary activities/complexity in tests - test only what you need
- Question randomness in tests - test explicitly what you want
- Don't add assertions for things you can assume work (e.g., "You are not testing TerminateWorkflowExecution here, you can assume it works")
- Remove redundant nil checks after you just set a value
- Do not export anything that doesn't need to be exported

## 2. Go Naming Conventions

- Don't use `Get` prefix for getters: `func (a *Activity) Store()` not `GetStore()`
- Don't use `Impl` suffix for implementations
- Don't put underscore after `Test` in test names: `TestRetry` not `Test_Retry`
- Avoid stuttering: don't use `ActivityStatus` in package `activity`, just `Status`
- Use `ok` boolean pattern instead of nil checks where idiomatic

## 3. Testify Suite Correctness and Reliability

- Never use `s.T()` in subtests - use the subtest's `t` parameter
- Never use suite assertion methods (`s.NoError`, `s.Equal`) from goroutines - causes panics
- Use `EventuallyWithT` when you need assertions inside eventually blocks, and use that block's `t`
- Use `require.ErrorAs(t, err, &specificErr)` for specific error type checks
- Prefer `require` over `assert` - it's rarely useful to continue a test after a failed assertion
- Add comments explaining why `Eventually` is needed (e.g., eventual consistency)
- Do not use single-value type assertions on errors (`err.(*T)`); this panics instead of failing the test when the type doesn't match. Use `errors.As` with a guarded return.
- When launching a goroutine to maintain a precondition for later assertions (e.g., keeping pollers active so a deployment version gets registered), loop until context cancellation rather than running once. A single attempt that times out exits silently, leaving downstream Eventually/propagation waits to hang until their own deadline.
- Never call testify assertions (`s.NoError`, `s.Equal`, `require.NoError`, even `assert.NoError`) inside a `go func()` — if the goroutine outlives the test, the assertion panics the binary with `panic: Fail in goroutine after TestXxx has completed`. Move assertions to the test goroutine or use a buffered error channel.
- Any `<-ch` that isn't inside a `select` with `ctx.Done()` will hang indefinitely if the sender never sends. Always provide a context cancellation fallback.
- Never write to package-level or global variables in tests — parallel tests share the same process; thread values through function parameters instead.
- Never use `time.Sleep` or `time.Since(start) > threshold` to enforce ordering — use channels, `sync.WaitGroup`, or `EventuallyWithT` instead.
- When using `EventuallyWithT` (or similar) to wait for a condition driven by a background goroutine, ensure the goroutine's timeout is longer than the `EventuallyWithT` deadline — if the background op times out first, the condition will never be satisfied and the wait will hang until its own deadline.
- Do not silently discard errors from precondition operations with `_, _ = f()` — if `f()` failing invalidates the rest of the test, surface the error or loop until it succeeds.
- Be suspicious of `go s.someHelper(ctx, ...)` calls where the goroutine runs exactly once and the test then immediately waits for something that helper was supposed to cause. If the operation can fail transiently (network, tight deadline, busy CI), the single attempt may fail silently and the wait will never succeed. Either loop the goroutine until `ctx.Done()`, or check that the operation succeeded before proceeding.

## 4. Inline Code / Avoid Abstractions

- Repeat strings instead of adding constants for single use
- Inline struct field assignments when possible
- Avoid unnecessary wrapper types and generic structs
- Don't add dependencies for 5 lines of code - "just write 5 lines of code instead of adding more dependency bloat"
- Don't create testsuite-level helpers that can't be safely used in subtests
- Prefer explicit code over reflection

## 5. Proper Error Handling

- Use standard error types (`InvalidArgument`, `NotFound`, `FailedPrecondition`) over custom error types
- Mark errors as non-retryable when task shouldn't retry in queue
- Wrap errors with context when there's something interesting or informative to add, e.g. `fmt.Errorf("multi-operation part 2: %w", err)`
- Don't panic in library code - return errors and let caller decide
- Validate early in handlers, not deep in business logic
- Use `errors.AsType` instead of `errors.As`
- Use `require.ErrorContains` instead of two separate assertions (`require.Error` + `require.Contains`)

## 6. Consistency with Codebase

- Follow existing patterns: "We have been passing through the frontend request in other libraries. Let's keep the same pattern here"
- Use existing utilities before creating new ones
- Follow CLI documentation conventions (capitalize proper nouns)
- Match existing metric tag formats (CONSTANT_CASE for enum values)
- Use the same error message style (no punctuation for single sentences)

## 7. Code comments

- A comment should be removed if the behavior of the code without the comment should be apparent to a reader familiar with the codebase.
- If the benefit of a comment can be achieved by improving variable/function names then suggest that.
- A comment must not give unnecessary or verbose explanation.
- A comment must not use language that is metaphorical or alien to the codebase.
- Sentence structure in comments should be simple. Prefer several plain statements over one sentence built from subordinate clauses, parentheticals, or stacked qualifications.
- A comment should typically not refer to counterfactuals, or to discussions or decision processes that occurred when the code was written.
- A comment should typically not explain how upstream callers use the code.
- Give the concrete replacement text as a code suggestion. If the clearer and shorter fix is to restructure the code rather than reword the comment, suggest that code instead.

## 8. API and Proto Design

- Document all proto fields with comments
- Use proper field names: `request_id` not `requestId`, `schedule_time` not `scheduledTime`
- Don't expose internal concepts in user-facing errors: "LowCardinalityKeyword is not a user facing concept"
- Accept event attributes structs instead of growing function signatures
- Prefer enums over int/string for well-known values

## 9. Concurrency and Safety

- Prefer immutable data patterns (for normal structs and especially proto messages) to avoid data races and synchronization
- Default to `sync.Mutex` for synchronization; atomics are an advanced tool for specific patterns or performance concerns
- Prefer `sync.Mutex` over `sync.RWMutex` almost always, except when reads are much more common than writes (>1000×) or readers hold the lock for significant time
- Don't do IO while holding locks - use side effect tasks
- Clone data before releasing locks if it might be modified
- Proto message fields accessed outside the workflow lock must be cloned, not aliased: use `common.CloneProto(...)` rather than returning the pointer directly.

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

Report findings at all four severity levels.
Prefer a small number of high-confidence findings.
Keep `nit` and `small` findings proportionally shorter than `med` and `high` findings.
Report concrete `nit` and `small` findings selectively, and consolidate related symptoms into a single comment that addresses the root issue.

### Feedback style

Be direct and practical, without fluff.
Reference specific codebase patterns and utilities, suggest concrete alternatives, and explain why something should change, not just that it should.
