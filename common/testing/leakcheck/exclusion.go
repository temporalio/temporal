package leakcheck

import (
	"fmt"
	"strings"
)

type exclusion struct {
	pattern string
	matched bool
}

type exclusions []exclusion

func newExclusion(pattern string) (exclusion, error) {
	if hasSpecificPathIndex(pattern) {
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

func hasSpecificPathIndex(path string) bool {
	for i := 0; i < len(path); {
		if path[i] != '[' {
			i++
			continue
		}

		end := strings.IndexByte(path[i:], ']')
		if end < 0 {
			return false
		}
		end += i
		index := path[i+1 : end]
		keyIndex, hasKeyPrefix := strings.CutPrefix(index, "key")
		if allDigits(index) || hasKeyPrefix && allDigits(keyIndex) {
			return true
		}
		i = end + 1
	}
	return false
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
