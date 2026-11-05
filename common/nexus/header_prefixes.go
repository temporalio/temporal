package nexus

import (
	"slices"
	"strings"
)

// HeaderPrefixMatch is a header key prefix and the sorted header keys that start with it.
type HeaderPrefixMatch struct {
	Prefix string
	Keys   []string
}

// HeaderPrefixMatches is a list of header prefix matches, in the order of the prefixes they were matched against.
type HeaderPrefixMatches []HeaderPrefixMatch

// MatchHeaderPrefixes returns a match for each of the given prefixes that at least one key of the given header starts
// with. Both the header keys and the prefixes are expected to be lower-cased.
func MatchHeaderPrefixes(lowerCaseHeader map[string]string, prefixes []string) HeaderPrefixMatches {
	var matches HeaderPrefixMatches
	for _, prefix := range prefixes {
		var keys []string
		for k := range lowerCaseHeader {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		if len(keys) > 0 {
			slices.Sort(keys)
			matches = append(matches, HeaderPrefixMatch{Prefix: prefix, Keys: keys})
		}
	}
	return matches
}

// Prefixes returns the matched prefixes.
func (m HeaderPrefixMatches) Prefixes() []string {
	prefixes := make([]string, len(m))
	for i, match := range m {
		prefixes[i] = match.Prefix
	}
	return prefixes
}

// Keys returns the sorted and de-duplicated header keys across all matches.
func (m HeaderPrefixMatches) Keys() []string {
	var keys []string
	for _, match := range m {
		keys = append(keys, match.Keys...)
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}
