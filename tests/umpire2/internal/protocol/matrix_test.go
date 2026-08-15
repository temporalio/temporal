package protocol

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
)

func TestActionCatalogPreservesDeclarationOrderAndIsDefensive(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	first := compiled.ActionCatalog()
	require.NotEmpty(t, first)
	require.Equal(t, modelActionKeys(), catalogKeys(first))
	first[0].Action.Name = "mutated"

	second := compiled.ActionCatalog()
	require.NotEqual(t, "mutated", second[0].Action.Name)
	require.True(t, slices.ContainsFunc(second, func(entry ActionCatalogEntry) bool { return entry.GapReason != "" }))
}

func TestGenerateMatrixIsDeterministicIncludesGapsAndFiltersCapabilities(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	catalog := compiled.ActionCatalog()
	require.NotEmpty(t, catalog)
	requiredKey := catalog[0].Key
	profiles := []coreregress.Profile{
		{Name: "basic"},
		{Name: "enabled", Capabilities: []string{"special"}},
	}
	options := MatrixOptions{Requires: map[ActionKey][]string{requiredKey: {"special"}}}

	first, err := GenerateMatrix(compiled, profiles, options)
	require.NoError(t, err)
	second, err := GenerateMatrix(compiled, profiles, options)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.NotEmpty(t, first)
	require.True(t, slices.ContainsFunc(first, func(testCase MatrixCase) bool { return testCase.GapReason != "" && testCase.Action == nil }))
	require.False(t, slices.ContainsFunc(first, func(testCase MatrixCase) bool {
		return testCase.Profile.Name == "basic" && testCase.Key == requiredKey
	}))
	require.True(t, slices.ContainsFunc(first, func(testCase MatrixCase) bool {
		return testCase.Profile.Name == "enabled" && testCase.Key == requiredKey
	}))
	for _, testCase := range first {
		require.NotEmpty(t, testCase.Name)
		require.NotContains(t, testCase.Name, "/")
	}
	requireMatrixPairCoverage(t, catalog, profiles, options, first)
}

func requireMatrixPairCoverage(
	t *testing.T,
	catalog []ActionCatalogEntry,
	profiles []coreregress.Profile,
	options MatrixOptions,
	selected []MatrixCase,
) {
	t.Helper()
	want := map[string]struct{}{}
	for _, profile := range profiles {
		capabilities := map[string]struct{}{}
		for _, capability := range profile.Capabilities {
			capabilities[capability] = struct{}{}
		}
		for _, entry := range catalog {
			valid := true
			for _, required := range options.Requires[entry.Key] {
				if _, found := capabilities[required]; !found {
					valid = false
				}
			}
			if valid {
				addMatrixPairs(want, matrixValues(profile.Name, entry))
			}
		}
	}
	got := map[string]struct{}{}
	for _, testCase := range selected {
		entry := ActionCatalogEntry{Key: testCase.Key, Action: testCase.Action, GapReason: testCase.GapReason}
		addMatrixPairs(got, matrixValues(testCase.Profile.Name, entry))
	}
	for pair := range want {
		_, covered := got[pair]
		require.Truef(t, covered, "matrix pair is not covered: %s", pair)
	}
}

func matrixValues(profile string, entry ActionCatalogEntry) []string {
	return []string{
		"profile=" + profile,
		"entity=" + string(entry.Key.Entity),
		"edge=" + entry.Key.From + "/" + entry.Key.Event,
		"hosting=" + entry.Key.Hosting.String(),
		"action-or-gap=" + catalogActionValue(entry),
	}
}

func addMatrixPairs(target map[string]struct{}, values []string) {
	for left := 0; left < len(values); left++ {
		for right := left + 1; right < len(values); right++ {
			target[values[left]+"\x00"+values[right]] = struct{}{}
		}
	}
}

func TestGenerateMatrixIsPureAndRejectsInvalidInputs(t *testing.T) {
	compiled, err := Default()
	require.NoError(t, err)
	before := compiled.ActionCatalog()
	_, err = GenerateMatrix(compiled, []coreregress.Profile{{Name: "local"}})
	require.NoError(t, err)
	require.Equal(t, before, compiled.ActionCatalog())

	_, err = GenerateMatrix(nil, []coreregress.Profile{{Name: "local"}})
	require.ErrorContains(t, err, "protocol is nil")
	_, err = GenerateMatrix(compiled, nil)
	require.ErrorContains(t, err, "profiles are empty")
}

func modelActionKeys() []ActionKey {
	declaration := defaultDeclaration()
	var keys []ActionKey
	for _, entity := range declaration.Entities {
		for _, action := range entity.Actions {
			keys = append(keys, action.Key)
		}
		for _, gap := range entity.ActionGaps {
			keys = append(keys, gap.Key)
		}
	}
	return keys
}

func catalogKeys(catalog []ActionCatalogEntry) []ActionKey {
	keys := make([]ActionKey, len(catalog))
	for index, entry := range catalog {
		keys[index] = entry.Key
	}
	return keys
}
