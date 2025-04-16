// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
		for i, literal := range strings.Split(pattern, "*") {
			if i > 0 {
				// Replace * with .*
				result.WriteString(".*")
			}
			result.WriteString(regexp.QuoteMeta(literal))
		}
		result.WriteRune(')')
		if i < len(patterns)-1 {
			result.WriteRune('|')
		}
	}
	result.WriteRune('$')
	return regexp.Compile(result.String())
}
