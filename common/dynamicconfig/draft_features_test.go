package dynamicconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestDraftFieldsConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   any
		want    []string
		invalid bool
	}{
		{"strings", []string{"feature-a", " feature-b ", "feature-a"}, []string{"feature-a", " feature-b ", "feature-a"}, false},
		{"yaml-list", []any{"feature-a", "feature-b"}, []string{"feature-a", "feature-b"}, false},
		{"empty", []any{}, []string{}, false},
		{"null", nil, nil, true},
		{"null-member", []any{"feature-a", nil}, nil, true},
		{"number-member", []any{"feature-a", 42}, nil, true},
		{"bare-string", "feature-a", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertStrictStringList(tc.value)
			if tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
			collection := NewCollection(StaticClient{
				FrontendEnabledDraftFeatureAPIs.Key(): []ConstrainedValue{
					{Constraints: Constraints{Namespace: "tenant"}, Value: tc.value},
					{Value: []string{"global-feature"}},
				},
			}, log.NewNoopLogger())
			get := FrontendEnabledDraftFeatureAPIs.Get(collection)
			require.Equal(t, tc.want, get("tenant"))
			require.Equal(t, []string{"global-feature"}, get(""))
		})
	}
}

// Decode real YAML through LoadYamlFile. Each malformed namespace value sits
// above a valid namespace entry and an unconstrained entry that would enable
// features, so a converter that accepts the value (or keeps a prefix of it)
// returns a non-empty list.
func TestDraftFieldsConfigYAML(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  []string
	}{
		{"map", `{feature-a: true}`, nil},
		{"map-member", `[feature-a, {x: y}]`, nil},
		{"nested-list", `[[feature-a]]`, nil},
		{"nested-list-after-valid", `[feature-a, [feature-b]]`, nil},
		{"valid", `[feature-a, " Feature-B "]`, []string{"feature-a", " Feature-B "}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			yaml := "frontend.enabledDraftFeatureAPIs:\n" +
				"  - value: " + tc.value + "\n" +
				"    constraints:\n" +
				"      namespace: tenant\n" +
				"  - value: [feature-a, feature-b]\n" +
				"    constraints:\n" +
				"      namespace: tenant\n" +
				"  - value: [feature-a, feature-b]\n"
			result := LoadYamlFile([]byte(yaml))
			require.Empty(t, result.Errors)
			client := StaticClient{}
			for key, value := range result.Map {
				client[key] = value
			}
			get := FrontendEnabledDraftFeatureAPIs.Get(NewCollection(client, log.NewNoopLogger()))
			require.Equal(t, tc.want, get("tenant"))
			// Repeat to exercise the cached conversion.
			require.Equal(t, tc.want, get("tenant"))
			require.Equal(t, []string{"feature-a", "feature-b"}, get(""))
		})
	}
}
