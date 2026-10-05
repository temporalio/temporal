Before authoring or reviewing code, read and follow the [code guidelines](.github/copilot-instructions.md).

## Development

- Regenerate code when interface definitions change, including changes to `.proto` files or code annotated with `//go:generate`.
- Do not introduce new third-party libraries unless specifically requested.
- Check existing imports and `go.mod` before assuming a library is available.
- Leave `CONSIDER(name):` comments for future design considerations.
- Use `logger.Fatal` for core invariant violations and `logger.DPanic` for issues that are important but should not crash production.

## Testing

- Write tests for new functionality and run tests after altering code or tests. Start with unit tests for fastest feedback.
- Test both successful behavior and failure modes.
- Always include `-tags test_dep` when running tests. Include the `integration` tag only for integration tests.
- Avoid testify suites in unit tests; functional tests require suites for test cluster setup.
- For float comparisons, use `InDelta` or `InEpsilon` instead of `Equal` (enforced by `testifylint`).
- For error assertions in testify suites, use `s.Require().NoError(err)` instead of `s.NoError(err)` (enforced by `testifylint`).

## Commands

- Fast Go linting (changed packages): `make lint-code-fast`
- Full Go linting (all packages): `make lint-code`
- Formatting imports: `make fmt-imports`
- Code generation: `make proto`
- Update API proto: `make update-go-api`

## Primary Workflows

1. Inspect surrounding code, tests, and configuration before making changes.
2. Implement the change using existing patterns and regenerate code when required.
3. Determine the relevant test commands from repository documentation and build configuration, then run those tests and `make lint-code-fast` after code changes.
