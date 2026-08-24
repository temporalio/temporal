# Own Nexus Endpoint Operator Handlers

## Goal

Make the Nexus endpoint operator RPC implementations match the frontend Nexus CODEOWNERS rule introduced by the parent PR without changing runtime behavior.

## Design

Move `CreateNexusEndpoint`, `UpdateNexusEndpoint`, `DeleteNexusEndpoint`, `GetNexusEndpoint`, and `ListNexusEndpoints` from `service/frontend/operator_handler.go` into a new `service/frontend/nexus_endpoint_handler.go` file. The methods remain on `OperatorHandlerImpl` and keep their existing signatures, panic capture, and delegation to `NexusEndpointClient`, so construction, registration, request flow, and error handling are unchanged.

The new filename matches `/service/frontend/nexus_*`, assigning the extracted code to the Nexus owners while leaving unrelated operator APIs under their existing ownership. No new abstraction is needed because the existing handler and client already provide the appropriate interface boundary.

## Alternatives

- Assign all of `operator_handler.go` to the Nexus owners. This over-assigns unrelated operator APIs and is rejected.
- Add a special CODEOWNERS rule without moving code. CODEOWNERS only supports files and paths, not individual methods, so this cannot express the intended ownership.
- Duplicate the frontend CODEOWNERS rule in a standalone PR against `main`. This overlaps the parent PR and is rejected in favor of a focused stacked PR.

## Verification

This is a file-only refactor with no behavior change. Run the focused frontend package tests with the required `test_dep` tag, then run import formatting and the repository code linter. Confirm the final diff contains only the method move and this design record, with all existing comments preserved.

## Trade-offs and Failure Modes

The extraction adds one small source file but improves ownership precision without adding runtime complexity, load-sensitive behavior, security surface, or new failure modes. Compilation catches missing imports, duplicate methods, or an incomplete move; existing frontend tests exercise the unchanged handler wiring and delegation.
