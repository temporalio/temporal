package objectleak

import (
	"fmt"
	"regexp"
	"strings"
)

var specificPathIndexPattern = regexp.MustCompile(`\[(?:key)?[0-9]+\]`)

type exclusion struct {
	pattern string
	matched bool
}

type exclusions []exclusion

func newExclusion(pattern string) (exclusion, error) {
	if specificPathIndexPattern.MatchString(pattern) {
		return exclusion{}, fmt.Errorf("object exclusion %q targets a specific index; use [*] or [key*]", pattern)
	}
	return exclusion{pattern: pattern}, nil
}

func (xs exclusions) match(obj trackedObject) []string {
	var matches []string
	path := obj.path.normalized()
	for i := range xs {
		if xs[i].matches(path, obj.typeName) {
			xs[i].matched = true
			matches = append(matches, xs[i].pattern)
		}
	}
	return matches
}

func (e exclusion) matches(path string, typeName string) bool {
	return e.matchesValue(path) || e.matchesValue(typeName)
}

func (e exclusion) matchesValue(value string) bool {
	if prefix, ok := strings.CutSuffix(e.pattern, "*"); ok {
		return strings.HasPrefix(value, prefix)
	}
	return value == e.pattern
}
