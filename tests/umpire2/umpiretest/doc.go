// Package umpiretest is the author-facing Temporal adapter for Umpire.
//
// Ordinary behavioral tests should start with [RequireRegression] and the typed instructions under
// tests/umpire2/regress. Reusable runners use [RunRegression], [Plan], and [Judge] with explicit
// requests and results. Explicit action plans use the semantic ActionRunner in tests/umpire2.
// [NewCampaignExecutor] and [NewCanaryDriver] connect the same Temporal protocol and regression
// harness to the generic bounded campaign and guarded-canary engines.
//
// The lower-level planner, runtime, regression compiler, campaign, and canary packages remain
// available for custom environments. This package selects local test policy; it does not replace
// those generic interfaces.
package umpiretest
