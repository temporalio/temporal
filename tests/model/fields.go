// Package model holds shared RPCSpec field-option helpers and generated
// workflowservice helpers used across the per-entity model packages.
package model

import (
	"strings"
	"time"

	"go.temporal.io/server/tests/testcore/umpire"
)

// IsNexusWorkflowServiceMethod returns true for WorkflowService methods that
// touch Nexus operations. Used by the property test to scope mutation /
// registry strategies to the nexus-related call surface.
func IsNexusWorkflowServiceMethod(method string) bool {
	return strings.HasPrefix(method, "/temporal.api.workflowservice.v1.WorkflowService/") &&
		strings.Contains(method, "Nexus")
}

var (
	// NamespaceField is the standard field spec for a request's `namespace`
	// scalar field (must be non-empty; mutation produces an unknown namespace).
	NamespaceField = umpire.Field[string](
		NonZeroString("namespace"),
		umpire.Mutation[string]("wrong-namespace"),
	)

	// OperationIDField is the standard field spec for an operation_id (must
	// be non-empty; tagged as ID; mutation produces an unknown operation
	// reference).
	OperationIDField = umpire.Field[string](
		umpire.Role[string](umpire.RPCFieldRoleID),
		NonZeroString("operation_id"),
		umpire.Mutation[string]("wrong-operation-ref"),
	)

	// RunIDField is the standard field spec for a run_id (tagged as ID;
	// mutation produces an unknown operation reference).
	RunIDField = umpire.Field[string](
		umpire.Role[string](umpire.RPCFieldRoleID),
		umpire.Mutation[string]("wrong-operation-ref"),
	)

	// NondeterministicRunIDField is the field spec for a run_id that the
	// server returns and the client should treat as non-deterministic.
	NondeterministicRunIDField = umpire.Field[string](
		umpire.Role[string](umpire.RPCFieldRoleNonDeterministic),
	)

	// StartedRunIDField is the field spec for a run_id returned alongside a
	// successful start (non-deterministic but must be non-empty).
	StartedRunIDField = umpire.Field[string](
		umpire.Role[string](umpire.RPCFieldRoleNonDeterministic),
		NonZeroString("run_id"),
	)

	// IdentityField is the standard field spec for a caller identity string.
	IdentityField = umpire.Field[string](
		umpire.Role[string](umpire.RPCFieldRoleNonDeterministic),
		umpire.Mutable[string](),
	)

	// RequestIDField is the standard field spec for an idempotency
	// request_id (must be non-empty; tagged as ID; mutation produces an
	// unknown request).
	RequestIDField = umpire.Field[string](
		umpire.Role[string](umpire.RPCFieldRoleID),
		NonZeroString("request_id"),
		umpire.Mutation[string]("wrong-request-id"),
	)

	// ReasonField is the standard field spec for a free-form reason string
	// (mutable; non-asserted).
	ReasonField = umpire.Field[string](umpire.Mutable[string]())
)

// NonZeroString asserts the field's string value is non-empty.
func NonZeroString(name string) umpire.FieldOption[string] {
	return umpire.Assert[string](func(t *umpire.T, value string) {
		t.NotEmpty(value, name+" must be non-zero")
	})
}

// NonZeroDuration asserts the field's duration value is non-zero.
func NonZeroDuration(name string) umpire.FieldOption[time.Duration] {
	return umpire.Assert[time.Duration](func(t *umpire.T, value time.Duration) {
		t.NotZero(value, name+" must be non-zero")
	})
}
