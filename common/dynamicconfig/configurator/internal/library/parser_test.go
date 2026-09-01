package configurator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseExpression(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Expression
		wantErr bool
	}{
		// quoted string values
		{
			name:  "simple equality",
			input: `"env" = "prod"`,
			want:  &Expression{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
		},
		{
			name:  "simple inequality",
			input: `"env" != "prod"`,
			want:  &Expression{Key: "env", Operator: OpNotEqual, Value: StringValue("prod")},
		},
		{
			name:  "hyphenated value",
			input: `"region" = "us-west-1"`,
			want:  &Expression{Key: "region", Operator: OpEqual, Value: StringValue("us-west-1")},
		},
		{
			name:  "hyphenated key",
			input: `"shard-id" = "123"`,
			want:  &Expression{Key: "shard-id", Operator: OpEqual, Value: StringValue("123")},
		},
		{
			name:  "whitespace tolerance",
			input: `  "env"  =  "prod"  `,
			want:  &Expression{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
		},
		{
			name:  "keyword as value",
			input: `"env" = "or"`,
			want:  &Expression{Key: "env", Operator: OpEqual, Value: StringValue("or")},
		},
		{
			name:  "empty string value",
			input: `"env" = ""`,
			want:  &Expression{Key: "env", Operator: OpEqual, Value: StringValue("")},
		},

		// integer literal values
		{
			name:  "greater than integer",
			input: `"shard-id" > 100`,
			want:  &Expression{Key: "shard-id", Operator: OpGreater, Value: IntegerValue(100)},
		},
		{
			name:  "less than integer",
			input: `"shard-id" < 100`,
			want:  &Expression{Key: "shard-id", Operator: OpLess, Value: IntegerValue(100)},
		},
		{
			name:  "equality with integer",
			input: `"shard-id" = 42`,
			want:  &Expression{Key: "shard-id", Operator: OpEqual, Value: IntegerValue(42)},
		},
		{
			name:  "zero integer",
			input: `"count" = 0`,
			want:  &Expression{Key: "count", Operator: OpEqual, Value: IntegerValue(0)},
		},

		// float literal values
		{
			name:  "greater than float",
			input: `"ratio" > 0.5`,
			want:  &Expression{Key: "ratio", Operator: OpGreater, Value: FloatValue(0.5)},
		},
		{
			name:  "less than float",
			input: `"ratio" < 9.99`,
			want:  &Expression{Key: "ratio", Operator: OpLess, Value: FloatValue(9.99)},
		},
		{
			name:  "equality with float",
			input: `"threshold" = 1.5`,
			want:  &Expression{Key: "threshold", Operator: OpEqual, Value: FloatValue(1.5)},
		},

		// compound expressions
		{
			name:  "or expression",
			input: `"env" = "prod" or "env" = "staging"`,
			want: &Expression{
				Operator: OpOr,
				Subexpressions: []*Expression{
					{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
					{Key: "env", Operator: OpEqual, Value: StringValue("staging")},
				},
			},
		},
		{
			name:  "and expression",
			input: `"env" = "prod" and "region" = "us-west-1"`,
			want: &Expression{
				Operator: OpAnd,
				Subexpressions: []*Expression{
					{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
					{Key: "region", Operator: OpEqual, Value: StringValue("us-west-1")},
				},
			},
		},
		{
			name:  "numeric range with integers",
			input: `"shard-id" > 100 and "shard-id" < 200`,
			want: &Expression{
				Operator: OpAnd,
				Subexpressions: []*Expression{
					{Key: "shard-id", Operator: OpGreater, Value: IntegerValue(100)},
					{Key: "shard-id", Operator: OpLess, Value: IntegerValue(200)},
				},
			},
		},
		{
			name:  "grouped and-or",
			input: `("env" = "prod" and "region" = "us-west-1") or "env" = "staging"`,
			want: &Expression{
				Operator: OpOr,
				Subexpressions: []*Expression{
					{
						Operator: OpAnd,
						Subexpressions: []*Expression{
							{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
							{Key: "region", Operator: OpEqual, Value: StringValue("us-west-1")},
						},
					},
					{Key: "env", Operator: OpEqual, Value: StringValue("staging")},
				},
			},
		},
		{
			// and binds tighter than or, so this is (a and b) or c
			name:  "and-precedence over or",
			input: `"env" = "prod" and "region" = "us-west-1" or "env" = "staging"`,
			want: &Expression{
				Operator: OpOr,
				Subexpressions: []*Expression{
					{
						Operator: OpAnd,
						Subexpressions: []*Expression{
							{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
							{Key: "region", Operator: OpEqual, Value: StringValue("us-west-1")},
						},
					},
					{Key: "env", Operator: OpEqual, Value: StringValue("staging")},
				},
			},
		},
		{
			name:  "three-way or",
			input: `"env" = "prod" or "env" = "staging" or "env" = "dev"`,
			want: &Expression{
				Operator: OpOr,
				Subexpressions: []*Expression{
					{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
					{Key: "env", Operator: OpEqual, Value: StringValue("staging")},
					{Key: "env", Operator: OpEqual, Value: StringValue("dev")},
				},
			},
		},
		{
			name:  "no whitespace between tokens",
			input: `"env"="prod"and"region"="us-west-1"`,
			want: &Expression{
				Operator: OpAnd,
				Subexpressions: []*Expression{
					{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
					{Key: "region", Operator: OpEqual, Value: StringValue("us-west-1")},
				},
			},
		},
		{
			name:  "double-nested parens",
			input: `(("env" = "prod"))`,
			want:  &Expression{Key: "env", Operator: OpEqual, Value: StringValue("prod")},
		},

		// error cases
		{
			name:    "unquoted string value rejected",
			input:   "env = prod",
			wantErr: true,
		},
		{
			name:    "missing value",
			input:   `"env" =`,
			wantErr: true,
		},
		{
			name:    "unclosed paren",
			input:   `("env" = "prod"`,
			wantErr: true,
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: true,
		},
		{
			name:    "operator only",
			input:   `=`,
			wantErr: true,
		},
		{
			name:    "missing closing quote",
			input:   `"env" = "prod`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseExpression(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
