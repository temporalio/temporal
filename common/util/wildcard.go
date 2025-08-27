package util

import (
	"errors"
	"regexp"
	"strings"
)

// WildCardStringToRegexp converts a given string pattern to a regular expression matching wildcards (*) with any
// substring.
func WildCardStringToRegexp(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, errors.New("pattern cannot be empty")
	}
	return WildCardStringsToRegexp([]string{pattern})
}

// WildCardStringToRegexps converts a given slices of string patterns to a slice of regular expressions matching
// wildcards (*) with any substring.
func WildCardStringsToRegexp(patterns []string) (*regexp.Regexp, error) {
	var result strings.Builder
	result.WriteRune('^')
	for i, pattern := range patterns {
		result.WriteRune('(')
		first := true
		for literal := range strings.SplitSeq(pattern, "*") {
			if !first {
				// Replace * with .*
				result.WriteString(".*")
			}
			result.WriteString(regexp.QuoteMeta(literal))
			first = false
		}
		result.WriteRune(')')
		if i < len(patterns)-1 {
			result.WriteRune('|')
		}
	}
	result.WriteRune('$')
	return regexp.Compile(result.String())
}
