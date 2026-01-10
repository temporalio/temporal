# Code Review Guidelines

Apply these patterns when reviewing PRs or suggesting code changes.

## 1. Remove Redundant Code (Highest Priority)

- Remove code that doesn't add value to tests or implementation
- Don't add unnecessary activities/complexity in tests - test only what you need
- Avoid redundant checks for atomic operations (e.g., "reset is atomic, no need for eventually")
- Question randomness in tests - test explicitly what you want
- Don't add assertions for things you can assume work (e.g., "You are not testing TerminateWorkflowExecution here, you can assume it works")
- Remove redundant nil checks after you just set a value

## 2. Go Naming Conventions

- Don't use `Get` prefix for getters: `func (a *Activity) Store()` not `GetStore()`
- Don't use `Impl` suffix for implementations
- Don't put underscore after `Test` in test names: `TestRetry` not `Test_Retry`
- Avoid stuttering: don't use `ActivityStatus` in package `activity`, just `Status`
- Use `ok` boolean pattern instead of nil checks where idiomatic

## 3. Test Suite Correctness

- Never use `s.T()` in subtests - use the subtest's `t` parameter
- Never use suite assertion methods (`s.NoError`, `s.Equal`) from goroutines - causes panics
- Use `EventuallyWithT` when you need assertions inside eventually blocks, and use that block's `t`
- Use `require.ErrorAs(t, err, &specificErr)` for specific error type checks
- Don't use `assert` when you need to stop execution - use `require`
- Add comments explaining why `Eventually` is needed (e.g., eventual consistency)

## 4. Inline Code / Avoid Abstractions

- Repeat strings instead of adding constants for single use: "nit: I would just repeat the string instead of adding a layer of indirection"
- Inline struct field assignments when possible
- Avoid unnecessary wrapper types and generic structs
- Don't add dependencies for 5 lines of code - "just write 5 lines of code instead of adding more dependency bloat"
- Don't create testsuite-level helpers that can't be safely used in subtests
- Prefer explicit code over reflection: "It would be easier to follow without all of the reflection"

## 5. Proper Error Handling

- Use standard error types (`InvalidArgument`, `NotFound`, `FailedPrecondition`) over custom error types
- Mark errors as non-retryable when task shouldn't retry in queue
- Always provide context with errors: `fmt.Errorf("context: %w", err)`
- Use early returns over nested if/else blocks
- Don't panic in library code - return errors and let caller decide
- Validate early in handlers, not deep in business logic

## 6. Consistency with Codebase

- Follow existing patterns: "We have been passing through the frontend request in other libraries. Let's keep the same pattern here"
- Don't separate temporal/non-temporal imports with blank lines (team decision)
- Use existing utilities before creating new ones
- Follow CLI documentation conventions (capitalize proper nouns)
- Match existing metric tag formats (CONSTANT_CASE for enum values)
- Use the same error message style (no punctuation for single sentences)

## 7. API and Proto Design

- Document all proto fields with comments
- Use proper field names: `request_id` not `requestId`, `schedule_time` not `scheduledTime`
- Don't expose internal concepts in user-facing errors: "LowCardinalityKeyword is not a user facing concept"
- Accept event attributes structs instead of growing function signatures
- Use `map[string]Empty` over `[]string` when you might add metadata later
- Prefer enums over int/string for well-known values

## 8. Concurrency and Safety

- Use `atomic.Value` for concurrent access, not regular variables
- Don't do IO while holding locks - use immediate tasks instead
- Clone data before releasing locks if it might be modified
- Use `sync.Mutex` over `sync.RWMutex` when you only read once before writing
