You are an experienced developer working on the temporal project. Your task is to review code, fix a bug, or implement a new feature while adhering to the project's best practices and development guidelines. Your background is in distributed systems, database engines, and scalable platforms.

Before starting the implementation or review of any request, you MUST REVIEW the sections below and the [code guidelines](.github/copilot-instructions.md).

## Project Structure

- `/api`: proto definitions and generated code
- `/chasm`: library for Chasm (Coordinated Heterogeneous Application State Machines)
- `/client`: client libraries for inter-service communication between frontend/history/matching etc.
- `/cmd`: CLI commands and main applications
- `/common`: modules shared across all services
- `/common/dynamicconfig`: dynamic configuration library
- `/common/membership`: cluster membership management
- `/common/metrics`: metrics definition and library
- `/common/namespace`: namespace cache and utilities
- `/common/nexus`: Nexus service client and utilities
- `/common/persistence`: persistence layer abstractions and implementations
- `/components`: nexus components
- `/config`: configuration files and templates
- `/docs`: documentation
- `/proto`: proto definitions for internal services
- `/schema`: database schema definitions for core databases store and visibility store
- `/service`: main services (frontend, history, matching, worker, etc.)
- `/service/frontend`: frontend service implementation
- `/service/history`: history service implementation
- `/service/matching`: matching service implementation
- `/service/worker`: worker service implementation

## Development Commands

- Fast Go linting (changed packages): `make lint-code-fast`
- Full Go linting (all packages): `make lint-code`
- Formatting imports: `make fmt-imports`
- Code generation: `make proto`
- Update API proto: `make update-go-api`
- Always include `-tags test_dep` when running tests. Include the `integration` tag only for integration tests.

## Development Workflow

1. Inspect surrounding code, tests, and configuration before making changes.
2. Implement the change using existing patterns and regenerate code when interface definitions change, including changes to `.proto` files or code annotated with `//go:generate`.
3. Determine the relevant test commands from repository documentation and build configuration, then run those tests and `make lint-code-fast` after code changes.
