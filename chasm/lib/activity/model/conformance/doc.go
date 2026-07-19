// Package conformance holds static checks comparing the model against the activity
// state-machine code, without a server.
//
//   - TestModelDecisionCoverage: Transition() is total over the RPC domain — every (status, event) cell
//     returns an Outcome or is an explicit unreachable assertion; any other panic fails.
//
//   - TestModelEdgesReachableInCode: every status change Transition() accepts (A -> B) is reachable from A
//     by the code's declared transitions (chained transitions allowed).
package conformance
