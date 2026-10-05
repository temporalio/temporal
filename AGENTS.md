Before authoring or reviewing code, read and follow the [code guidelines](.github/copilot-instructions.md).

## Development Commands

- Fast Go linting (changed packages): `make lint-code-fast`
- Full Go linting (all packages): `make lint-code`
- Formatting imports: `make fmt-imports`
- Code generation: `make proto`
- Update API proto: `make update-go-api`

## Development Workflow

1. Inspect surrounding code, tests, and configuration before making changes.
2. Implement the change using existing patterns and regenerate code when interface definitions change, including changes to `.proto` files or code annotated with `//go:generate`.
3. Determine the relevant test commands from repository documentation and build configuration, then run those tests and `make lint-code-fast` after code changes.
