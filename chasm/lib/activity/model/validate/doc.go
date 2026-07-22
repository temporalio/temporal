// Package validate holds static checks validating the model against the activity
// state-machine code, without a server. (Distinct from conformance testing, which checks a running
// implementation against the model; here we check the model itself against the code's structure.)
//
//   - TestModelDecisionCoverage: Transition() is total over the RPC domain — every (status, event) cell
//     returns an Outcome or is an explicit unreachable assertion; any other panic fails.
//
//   - TestModelEdgesReachableInCode: every status change Transition() accepts (A -> B) is reachable from A
//     by the code's declared transitions (chained transitions allowed).
package validate
