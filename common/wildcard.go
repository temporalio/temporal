package common

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
	var result strings.Builder
	result.WriteString("^")
	for i, literal := range strings.Split(pattern, "*") {
		if i > 0 {
			// Replace * with .*
			result.WriteString(".*")
		}
		result.WriteString(regexp.QuoteMeta(literal))
	}
	result.WriteString("$")
	return regexp.Compile(result.String())
}

// WildCardStringToRegexps converts a given slices of string patterns to a slice of regular expressions matching
// wildcards (*) with any substring.
func WildCardStringsToRegexps(patterns []string) ([]*regexp.Regexp, error) {
	var errs []error
	regexps := make([]*regexp.Regexp, len(patterns))
	for i, pattern := range patterns {
		re, err := WildCardStringToRegexp(pattern)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		regexps[i] = re
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return regexps, nil
}
