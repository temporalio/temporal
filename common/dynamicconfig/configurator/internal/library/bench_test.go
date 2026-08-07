package configurator

import (
	"context"
	"testing"

	"go.temporal.io/server/common/dynamicconfig/configurator/types"
)

// Sink prevents the compiler from eliding benchmark calls.
var Sink string

// yamlLevel represents one level of a nested YAML config tree.
// At each level a single constraint key is checked; matched values either
// return a result (leaf) or descend to the next level (branch).
type yamlLevel struct {
	constraintKey string
	values        map[string]yamlValue
	defaultValue  string
}

type yamlValue struct {
	result string     // set on leaf nodes
	next   *yamlLevel // set on branch nodes
}

func (l *yamlLevel) eval(constraints types.Constraints) string {
	raw, ok := constraints[l.constraintKey]
	if !ok {
		return l.defaultValue
	}
	v, ok := raw.(string)
	if !ok {
		return l.defaultValue
	}
	entry, ok := l.values[v]
	if !ok {
		return l.defaultValue
	}
	if entry.next != nil {
		return entry.next.eval(constraints)
	}
	return entry.result
}

// buildYAMLConfig constructs the nested-map equivalent of:
//
//	env:
//	  prod:
//	    region:
//	      us-west-1:
//	        account:
//	          foo: "deploy"
//	          bar: "deploy"
//	      us-west-2: "do-not-deploy"
//	  staging: "deploy"
//	default: "do-not-deploy"
func buildYAMLConfig() *yamlLevel {
	return &yamlLevel{
		constraintKey: "env",
		defaultValue:  "do-not-deploy",
		values: map[string]yamlValue{
			"staging": {result: "deploy"},
			"prod": {next: &yamlLevel{
				constraintKey: "region",
				defaultValue:  "do-not-deploy",
				values: map[string]yamlValue{
					"us-west-2": {result: "do-not-deploy"},
					"us-west-1": {next: &yamlLevel{
						constraintKey: "account",
						defaultValue:  "do-not-deploy",
						values: map[string]yamlValue{
							"foo": {result: "deploy"},
							"bar": {result: "deploy"},
						},
					}},
				},
			}},
		},
	}
}

// buildDSLConfig loads the DSL-evaluator equivalent of the same config.
// Each distinct leaf becomes a separate override checked in order:
//
//	Override 1: env=prod AND region=us-west-1 AND account=foo → "deploy"
//	Override 2: env=prod AND region=us-west-1 AND account=bar → "deploy"
//	Override 3: env=staging                                   → "deploy"
//	Default:                                                  → "do-not-deploy"
func buildDSLConfig() *configurator[string] {
	c := New[string]()
	data := types.Config[string]{
		DefaultValue: "do-not-deploy",
		Overrides: []types.Override[string]{
			{
				MatchString: `"env" = "prod" and "region" = "us-west-1" and "account" = "foo"`,
				MatchResult: "deploy",
			},
			{
				MatchString: `"env" = "prod" and "region" = "us-west-1" and "account" = "bar"`,
				MatchResult: "deploy",
			},
			{
				MatchString: `"env" = "staging"`,
				MatchResult: "deploy",
			},
		},
	}
	if err := c.Load("flag", data); err != nil {
		panic(err)
	}
	return c
}

// Shared constraint sets — allocated once, reused across benchmark iterations.
var (
	// Hits the deepest path: prod → us-west-1 → account foo
	constraintsDeepMatch = types.Constraints{"env": "prod", "region": "us-west-1", "account": "foo"}
	// Hits early: staging exits at the first level of the YAML tree, but only
	// after two full override misses in the DSL evaluator.
	constraintsStagingMatch = types.Constraints{"env": "staging"}
	// Falls through to default at the account level (valid env+region, unknown account).
	constraintsNoMatchAccount = types.Constraints{"env": "prod", "region": "us-west-1", "account": "baz"}
	// Falls through to default at the region level.
	constraintsNoMatchRegion = types.Constraints{"env": "prod", "region": "us-east-1"}
)

// --- Deep match: prod / us-west-1 / foo ---

func BenchmarkEval_DeepMatch(b *testing.B) {
	c := buildDSLConfig()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sink, _ = c.Eval(ctx, "flag", constraintsDeepMatch)
	}
}

func BenchmarkYAMLMap_DeepMatch(b *testing.B) {
	m := buildYAMLConfig()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sink = m.eval(constraintsDeepMatch)
	}
}

// --- Staging match: early YAML exit, late DSL exit ---

func BenchmarkEval_StagingMatch(b *testing.B) {
	c := buildDSLConfig()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sink, _ = c.Eval(ctx, "flag", constraintsStagingMatch)
	}
}

func BenchmarkYAMLMap_StagingMatch(b *testing.B) {
	m := buildYAMLConfig()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sink = m.eval(constraintsStagingMatch)
	}
}

// --- No-match: unknown account (exhausts all overrides in DSL) ---

func BenchmarkEval_NoMatch_Account(b *testing.B) {
	c := buildDSLConfig()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sink, _ = c.Eval(ctx, "flag", constraintsNoMatchAccount)
	}
}

func BenchmarkYAMLMap_NoMatch_Account(b *testing.B) {
	m := buildYAMLConfig()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sink = m.eval(constraintsNoMatchAccount)
	}
}

// --- No-match: wrong region (DSL short-circuits on first and-subexpr mismatch) ---

func BenchmarkEval_NoMatch_Region(b *testing.B) {
	c := buildDSLConfig()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sink, _ = c.Eval(ctx, "flag", constraintsNoMatchRegion)
	}
}

func BenchmarkYAMLMap_NoMatch_Region(b *testing.B) {
	m := buildYAMLConfig()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Sink = m.eval(constraintsNoMatchRegion)
	}
}
