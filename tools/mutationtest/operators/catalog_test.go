package operators

import (
	"go/ast"
	"go/types"
	"testing"

	"github.com/avito-tech/go-mutesting/mutator"
	"github.com/stretchr/testify/require"
)

func TestResolveDefaultsPreservesCuratedOperators(t *testing.T) {
	t.Parallel()

	resolved, err := Resolve("", "")
	require.NoError(t, err)
	require.Equal(t, []string{
		"arithmetic/assign_invert",
		"arithmetic/assignment",
		"arithmetic/base",
		"arithmetic/bitwise",
		"branch/case",
		"branch/else",
		"branch/if",
		"conditional/negated",
		"expression/comparison",
		"loop/break",
		"loop/condition",
		"loop/range_break",
		"numbers/decrementer",
		"numbers/incrementer",
	}, operatorNames(resolved))
}

func TestResolveExpandsSelectors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		include string
		exclude string
		want    []string
	}{
		{
			name:    "exact replaces defaults",
			include: "branch/if",
			want:    []string{"branch/if"},
		},
		{
			name:    "category replaces defaults",
			include: "branch",
			want:    []string{"branch/case", "branch/else", "branch/if"},
		},
		{
			name:    "default includes opt in",
			include: "default boolean/literal",
			want: []string{
				"arithmetic/assign_invert",
				"arithmetic/assignment",
				"arithmetic/base",
				"arithmetic/bitwise",
				"boolean/literal",
				"branch/case",
				"branch/else",
				"branch/if",
				"conditional/negated",
				"expression/comparison",
				"loop/break",
				"loop/condition",
				"loop/range_break",
				"numbers/decrementer",
				"numbers/incrementer",
			},
		},
		{
			name:    "all is explicit catalog",
			include: "all",
			want: []string{
				"arithmetic/assign_invert",
				"arithmetic/assignment",
				"arithmetic/base",
				"arithmetic/bitwise",
				"boolean/literal",
				"branch/case",
				"branch/else",
				"branch/if",
				"conditional/negated",
				"expression/comparison",
				"loop/break",
				"loop/condition",
				"loop/range_break",
				"numbers/decrementer",
				"numbers/incrementer",
			},
		},
		{
			name:    "exact exclusion wins",
			include: "branch",
			exclude: "branch/else",
			want:    []string{"branch/case", "branch/if"},
		},
		{
			name:    "category exclusion wins",
			include: "all",
			exclude: "arithmetic boolean conditional expression loop numbers",
			want:    []string{"branch/case", "branch/else", "branch/if"},
		},
		{
			name:    "overlap deduplicates canonically",
			include: "branch/if branch default branch/if",
			exclude: "arithmetic conditional expression loop numbers",
			want:    []string{"branch/case", "branch/else", "branch/if"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resolved, err := Resolve(tc.include, tc.exclude)
			require.NoError(t, err)
			require.Equal(t, tc.want, operatorNames(resolved))
		})
	}
}

func TestResolveRejectsInvalidSelections(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		include string
		exclude string
		wantErr string
	}{
		{name: "unknown exact", include: "branch/missing", wantErr: `unknown mutation selector "branch/missing"`},
		{name: "unknown category", include: "missing", wantErr: `unknown mutation selector "missing"`},
		{name: "malformed", include: "branch/if/extra", wantErr: `invalid mutation selector "branch/if/extra"`},
		{name: "empty result", include: "branch", exclude: "branch", wantErr: "mutation selection is empty"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := Resolve(tc.include, tc.exclude)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestNewCatalogRejectsInvalidDefinitions(t *testing.T) {
	t.Parallel()

	noop := func(*types.Package, *types.Info, ast.Node) []mutator.Mutation { return nil }
	testCases := []struct {
		name        string
		definitions []definition
		wantErr     string
	}{
		{
			name: "duplicate",
			definitions: []definition{
				{name: "branch/if", implementation: implementationUpstream, mutate: noop},
				{name: "branch/if", implementation: implementationLocal, mutate: noop},
			},
			wantErr: `duplicate mutation operator "branch/if"`,
		},
		{
			name:        "invalid name",
			definitions: []definition{{name: "branch", implementation: implementationLocal, mutate: noop}},
			wantErr:     `invalid mutation operator name "branch"`,
		},
		{
			name:        "whitespace in name",
			definitions: []definition{{name: "branch/if else", implementation: implementationLocal, mutate: noop}},
			wantErr:     `invalid mutation operator name "branch/if else"`,
		},
		{
			name:        "reserved category",
			definitions: []definition{{name: "all/operator", implementation: implementationLocal, mutate: noop}},
			wantErr:     `reserved mutation operator category "all"`,
		},
		{
			name:        "nil mutator",
			definitions: []definition{{name: "branch/if", implementation: implementationUpstream}},
			wantErr:     `mutation operator "branch/if" has a nil mutator`,
		},
		{
			name:        "invalid implementation",
			definitions: []definition{{name: "branch/if", implementation: "", mutate: noop}},
			wantErr:     `mutation operator "branch/if" has invalid implementation ""`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := newCatalog(tc.definitions)
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestListReturnsCanonicalDiscoveryMetadata(t *testing.T) {
	t.Parallel()

	descriptors, err := List()
	require.NoError(t, err)
	require.Len(t, descriptors, 15)
	require.Equal(t, Descriptor{
		Name:           "boolean/literal",
		Category:       "boolean",
		Default:        false,
		Implementation: "local",
	}, descriptors[4])
}

func operatorNames(resolved []Operator) []string {
	names := make([]string, 0, len(resolved))
	for _, operator := range resolved {
		names = append(names, operator.Name())
	}
	return names
}
