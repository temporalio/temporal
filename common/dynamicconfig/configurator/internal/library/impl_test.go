package configurator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig/configurator/types"
)

func TestE2E(t *testing.T) {
	configData := types.Config[string]{
		DefaultValue: "do-not-deploy",
		Overrides: []types.Override[string]{
			{
				MatchString: `("env" = "prod" and "region" = "us-west-1") or "env" = "staging"`,
				MatchResult: "deploy",
			},
		},
	}

	configstore := New[string]()
	require.NoError(t, configstore.Load("key", configData))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res, err := configstore.Eval(ctx, "key", types.Constraints{
		"env": "staging",
	})
	assert.NoError(t, err)
	assert.Equal(t, "deploy", res)

	res, err = configstore.Eval(ctx, "key", types.Constraints{
		"env": "not-staging",
	})
	assert.NoError(t, err)
	assert.Equal(t, "do-not-deploy", res)

	res, err = configstore.Eval(ctx, "key", types.Constraints{
		"env":    "prod",
		"region": "us-west-2",
	})
	assert.NoError(t, err)
	assert.Equal(t, "do-not-deploy", res)

	res, err = configstore.Eval(ctx, "key", types.Constraints{
		"env":      "prod",
		"shard-id": "123",
		"foo":      "bar",
		"region":   "us-west-1",
	})
	assert.NoError(t, err)
	assert.Equal(t, "deploy", res)

	// canary rollout: deploy only to shards in range 100 < shard-id < 200
	// uses unquoted integer literals parsed at load time
	canaryData := types.Config[string]{
		DefaultValue: "off",
		Overrides: []types.Override[string]{
			{
				MatchString: `"shard-id" > 100 and "shard-id" < 200`,
				MatchResult: "on",
			},
		},
	}
	require.NoError(t, configstore.Load("canary", canaryData))

	for _, tc := range []struct {
		shardID string
		want    string
	}{
		{"50", "off"},  // below range
		{"100", "off"}, // lower bound is exclusive
		{"150", "on"},  // inside range
		{"200", "off"}, // upper bound is exclusive
		{"250", "off"}, // above range
	} {
		res, err = configstore.Eval(ctx, "canary", types.Constraints{"shard-id": tc.shardID})
		assert.NoError(t, err)
		assert.Equal(t, tc.want, res, "shard-id=%s", tc.shardID)
	}

	// float threshold: rollout to users with ratio above 0.5
	// uses unquoted float literal parsed at load time
	floatData := types.Config[string]{
		DefaultValue: "off",
		Overrides: []types.Override[string]{
			{
				MatchString: `"ratio" > 0.5`,
				MatchResult: "on",
			},
		},
	}
	require.NoError(t, configstore.Load("float-flag", floatData))

	for _, tc := range []struct {
		ratio string
		want  string
	}{
		{"0.3", "off"},
		{"0.5", "off"}, // exclusive
		{"0.7", "on"},
		{"1.0", "on"},
	} {
		res, err = configstore.Eval(ctx, "float-flag", types.Constraints{"ratio": tc.ratio})
		assert.NoError(t, err)
		assert.Equal(t, tc.want, res, "ratio=%s", tc.ratio)
	}

	// native int constraint: same shard-id range but passed as int, not string
	for _, tc := range []struct {
		shardID int
		want    string
	}{
		{50, "off"},
		{100, "off"},
		{150, "on"},
		{200, "off"},
		{250, "off"},
	} {
		res, err = configstore.Eval(ctx, "canary", types.Constraints{"shard-id": tc.shardID})
		assert.NoError(t, err)
		assert.Equal(t, tc.want, res, "shard-id=%d (native int)", tc.shardID)
	}

	// native float64 constraint: same ratio threshold but passed as float64
	for _, tc := range []struct {
		ratio float64
		want  string
	}{
		{0.3, "off"},
		{0.5, "off"},
		{0.7, "on"},
		{1.0, "on"},
	} {
		res, err = configstore.Eval(ctx, "float-flag", types.Constraints{"ratio": tc.ratio})
		assert.NoError(t, err)
		assert.Equal(t, tc.want, res, "ratio=%v (native float64)", tc.ratio)
	}

	// bool config: values are carried through as-is, whatever their type
	boolConfigstore := New[bool]()
	boolData := types.Config[bool]{
		DefaultValue: false,
		Overrides: []types.Override[bool]{
			{MatchString: `"env" = "prod"`, MatchResult: true},
		},
	}
	require.NoError(t, boolConfigstore.Load("flag", boolData))
	boolRes, err := boolConfigstore.Eval(ctx, "flag", types.Constraints{"env": "prod"})
	assert.NoError(t, err)
	assert.True(t, boolRes)
	boolRes, err = boolConfigstore.Eval(ctx, "flag", types.Constraints{"env": "staging"})
	assert.NoError(t, err)
	assert.False(t, boolRes)
}

func TestExpression_Matches(t *testing.T) {
	tests := []struct {
		name        string
		expr        Expression
		constraints types.Constraints
		want        bool
	}{
		// = operator
		{
			name:        "equals: matching value",
			expr:        Expression{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
			constraints: types.Constraints{"env": "prod"},
			want:        true,
		},
		{
			name:        "equals: non-matching value",
			expr:        Expression{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
			constraints: types.Constraints{"env": "staging"},
			want:        false,
		},
		{
			name:        "equals: key absent",
			expr:        Expression{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
			constraints: types.Constraints{},
			want:        false,
		},

		// != operator
		{
			name:        "not-equals: different value",
			expr:        Expression{Key: "env", Operator: OpNotEqual, Value: StringValue("prod")},
			constraints: types.Constraints{"env": "staging"},
			want:        true,
		},
		{
			name:        "not-equals: same value",
			expr:        Expression{Key: "env", Operator: OpNotEqual, Value: StringValue("prod")},
			constraints: types.Constraints{"env": "prod"},
			want:        false,
		},
		{
			name:        "not-equals: key absent",
			expr:        Expression{Key: "env", Operator: OpNotEqual, Value: StringValue("prod")},
			constraints: types.Constraints{},
			want:        false,
		},

		// and operator
		{
			name: "and: all subexpressions match",
			expr: Expression{
				Operator: OpAnd,
				Subexpressions: []*Expression{
					{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
					{Key: "region", Operator: OpEqual, Value: StringValue("us-west-1")},
				},
			},
			constraints: types.Constraints{"env": "prod", "region": "us-west-1"},
			want:        true,
		},
		{
			name: "and: one subexpression does not match",
			expr: Expression{
				Operator: OpAnd,
				Subexpressions: []*Expression{
					{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
					{Key: "region", Operator: OpEqual, Value: StringValue("us-west-1")},
				},
			},
			constraints: types.Constraints{"env": "prod", "region": "eu-west-1"},
			want:        false,
		},
		{
			name:        "and: empty subexpressions",
			expr:        Expression{Operator: OpAnd, Subexpressions: []*Expression{}},
			constraints: types.Constraints{},
			want:        true,
		},

		// or operator
		{
			name: "or: one subexpression matches",
			expr: Expression{
				Operator: OpOr,
				Subexpressions: []*Expression{
					{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
					{Key: "env", Operator: OpEqual, Value: StringValue("staging")},
				},
			},
			constraints: types.Constraints{"env": "staging"},
			want:        true,
		},
		{
			name: "or: no subexpressions match",
			expr: Expression{
				Operator: OpOr,
				Subexpressions: []*Expression{
					{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
					{Key: "env", Operator: OpEqual, Value: StringValue("staging")},
				},
			},
			constraints: types.Constraints{"env": "dev"},
			want:        false,
		},
		{
			name:        "or: empty subexpressions",
			expr:        Expression{Operator: OpOr, Subexpressions: []*Expression{}},
			constraints: types.Constraints{},
			want:        false,
		},

		// != combined with and/or
		{
			name: "and with != subexpression",
			expr: Expression{
				Operator: OpAnd,
				Subexpressions: []*Expression{
					{Key: "env", Operator: OpNotEqual, Value: StringValue("prod")},
					{Key: "region", Operator: OpEqual, Value: StringValue("us-west-1")},
				},
			},
			constraints: types.Constraints{"env": "staging", "region": "us-west-1"},
			want:        true,
		},
		{
			name: "or with != subexpression that matches",
			expr: Expression{
				Operator: OpOr,
				Subexpressions: []*Expression{
					{Key: "env", Operator: OpNotEqual, Value: StringValue("prod")},
					{Key: "region", Operator: OpEqual, Value: StringValue("us-west-1")},
				},
			},
			constraints: types.Constraints{"env": "prod", "region": "eu-west-1"},
			want:        false,
		},

		// > with KindInteger (numeric comparison path)
		{
			name:        "greater-than: integer, constraint exceeds threshold",
			expr:        Expression{Key: "shard-id", Operator: OpGreater, Value: IntegerValue(100)},
			constraints: types.Constraints{"shard-id": "200"},
			want:        true,
		},
		{
			name:        "greater-than: integer, constraint equals threshold",
			expr:        Expression{Key: "shard-id", Operator: OpGreater, Value: IntegerValue(100)},
			constraints: types.Constraints{"shard-id": "100"},
			want:        false,
		},
		{
			name:        "greater-than: integer, constraint below threshold",
			expr:        Expression{Key: "shard-id", Operator: OpGreater, Value: IntegerValue(100)},
			constraints: types.Constraints{"shard-id": "50"},
			want:        false,
		},
		{
			name:        "greater-than: integer, key absent",
			expr:        Expression{Key: "shard-id", Operator: OpGreater, Value: IntegerValue(100)},
			constraints: types.Constraints{},
			want:        false,
		},

		// > with KindFloat (numeric comparison path)
		{
			name:        "greater-than: float, constraint exceeds threshold",
			expr:        Expression{Key: "ratio", Operator: OpGreater, Value: FloatValue(0.5)},
			constraints: types.Constraints{"ratio": "0.7"},
			want:        true,
		},
		{
			name:        "greater-than: float, constraint equals threshold",
			expr:        Expression{Key: "ratio", Operator: OpGreater, Value: FloatValue(0.5)},
			constraints: types.Constraints{"ratio": "0.5"},
			want:        false,
		},

		// > with KindString (lexicographic comparison path)
		{
			name:        "greater-than: string lexicographic",
			expr:        Expression{Key: "env", Operator: OpGreater, Value: StringValue("prod")},
			constraints: types.Constraints{"env": "staging"}, // "staging" > "prod"
			want:        true,
		},

		// < with KindInteger
		{
			name:        "less-than: integer, constraint below threshold",
			expr:        Expression{Key: "shard-id", Operator: OpLess, Value: IntegerValue(100)},
			constraints: types.Constraints{"shard-id": "50"},
			want:        true,
		},
		{
			name:        "less-than: integer, constraint equals threshold",
			expr:        Expression{Key: "shard-id", Operator: OpLess, Value: IntegerValue(100)},
			constraints: types.Constraints{"shard-id": "100"},
			want:        false,
		},
		{
			name:        "less-than: integer, constraint exceeds threshold",
			expr:        Expression{Key: "shard-id", Operator: OpLess, Value: IntegerValue(100)},
			constraints: types.Constraints{"shard-id": "200"},
			want:        false,
		},
		{
			name:        "less-than: integer, key absent",
			expr:        Expression{Key: "shard-id", Operator: OpLess, Value: IntegerValue(100)},
			constraints: types.Constraints{},
			want:        false,
		},

		// < with KindFloat
		{
			name:        "less-than: float, constraint below threshold",
			expr:        Expression{Key: "ratio", Operator: OpLess, Value: FloatValue(0.5)},
			constraints: types.Constraints{"ratio": "0.3"},
			want:        true,
		},

		// < with KindString
		{
			name:        "less-than: string lexicographic",
			expr:        Expression{Key: "env", Operator: OpLess, Value: StringValue("staging")},
			constraints: types.Constraints{"env": "prod"}, // "prod" < "staging"
			want:        true,
		},

		// native int constraints
		{
			name:        "greater-than: integer, native int constraint exceeds threshold",
			expr:        Expression{Key: "shard-id", Operator: OpGreater, Value: IntegerValue(100)},
			constraints: types.Constraints{"shard-id": 150},
			want:        true,
		},
		{
			name:        "greater-than: integer, native int constraint equals threshold",
			expr:        Expression{Key: "shard-id", Operator: OpGreater, Value: IntegerValue(100)},
			constraints: types.Constraints{"shard-id": 100},
			want:        false,
		},
		{
			name:        "less-than: integer, native int constraint below threshold",
			expr:        Expression{Key: "shard-id", Operator: OpLess, Value: IntegerValue(100)},
			constraints: types.Constraints{"shard-id": 50},
			want:        true,
		},
		{
			name:        "equals: integer, native int matches",
			expr:        Expression{Key: "shard-id", Operator: OpEqual, Value: IntegerValue(100)},
			constraints: types.Constraints{"shard-id": 100},
			want:        true,
		},

		// native float64 constraints
		{
			name:        "greater-than: float, native float64 constraint exceeds threshold",
			expr:        Expression{Key: "ratio", Operator: OpGreater, Value: FloatValue(0.5)},
			constraints: types.Constraints{"ratio": 0.7},
			want:        true,
		},
		{
			name:        "less-than: float, native float64 constraint below threshold",
			expr:        Expression{Key: "ratio", Operator: OpLess, Value: FloatValue(0.5)},
			constraints: types.Constraints{"ratio": 0.3},
			want:        true,
		},

		// native bool constraints
		{
			name:        "equals: bool, true matches string 'true'",
			expr:        Expression{Key: "enabled", Operator: OpEqual, Value: StringValue("true")},
			constraints: types.Constraints{"enabled": true},
			want:        true,
		},
		{
			name:        "equals: bool, false does not match string 'true'",
			expr:        Expression{Key: "enabled", Operator: OpEqual, Value: StringValue("true")},
			constraints: types.Constraints{"enabled": false},
			want:        false,
		},

		// unknown operator
		{
			name:        "unknown operator returns false without error",
			expr:        Expression{Key: "env", Operator: types.Operator(0), Value: StringValue("prod")},
			constraints: types.Constraints{"env": "prod"},
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.expr.Matches(tt.constraints)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
