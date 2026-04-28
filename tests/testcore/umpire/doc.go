// Package umpire is a property-based testing toolkit for gRPC-backed
// systems. It records observed traffic as facts, checks safety and liveness
// rules over that history, injects faults and request mutations through a
// composable interceptor chain, and tracks rule coverage across runs.
//
// # Error-handling convention
//
// The package distinguishes wiring mistakes from runtime conditions:
//
//   - Construction- and registration-time wiring mistakes panic. These are
//     programmer errors fixed in the test setup, not conditions a test can
//     recover from, so failing fast at init is preferable to threading errors
//     through every call. Examples: RPCRegistry.Register, NewRPCFieldRef,
//     BuildRPCSpec, RuleSet.Register / Add.
//
//   - Conditions a caller might reasonably inspect or report are returned as
//     errors. Examples: Components.Validate / NewComponents and the internal
//     RPCRegistry.register / RPCSpec.validate that Register wraps.
package umpire
