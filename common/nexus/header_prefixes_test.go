package nexus

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchHeaderPrefixes(t *testing.T) {
	header := map[string]string{
		"key":            "v",
		"x-temporal-foo": "v",
		"temporal-b":     "v",
		"temporal-a":     "v",
		"temporal-x-c":   "v",
		"tmprl-d":        "v",
	}

	require.Empty(t, MatchHeaderPrefixes(nil, []string{"temporal-"}))
	require.Empty(t, MatchHeaderPrefixes(header, nil))
	require.Empty(t, MatchHeaderPrefixes(header, []string{"unused-"}))

	matches := MatchHeaderPrefixes(header, []string{"unused-", "temporal-x-", "tmprl-", "temporal-"})
	require.Equal(t, HeaderPrefixMatches{
		{Prefix: "temporal-x-", Keys: []string{"temporal-x-c"}},
		{Prefix: "tmprl-", Keys: []string{"tmprl-d"}},
		{Prefix: "temporal-", Keys: []string{"temporal-a", "temporal-b", "temporal-x-c"}},
	}, matches)
	require.Equal(t, []string{"temporal-x-", "tmprl-", "temporal-"}, matches.Prefixes())
	require.Equal(t, []string{"temporal-a", "temporal-b", "temporal-x-c", "tmprl-d"}, matches.Keys())
}
