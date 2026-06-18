package leakcheck

import "strings"

type exclusion struct {
	pattern string
	invalid bool
	matched bool
}

func newExclusion(pattern string) exclusion {
	return exclusion{
		pattern: pattern,
		invalid: hasSpecificPathIndex(pattern),
	}
}

func matchingExcludes(obj trackedObject, exclusions []exclusion) []string {
	var matches []string
	path := obj.path.Normalized()
	for i := range exclusions {
		if exclusions[i].matches(path, obj.typeName) {
			exclusions[i].matched = true
			matches = append(matches, exclusions[i].pattern)
		}
	}
	return matches
}

func (e exclusion) matches(path string, typeName string) bool {
	return !e.invalid && (e.matchesValue(path) || e.matchesValue(typeName))
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
		if allDigits(index) || strings.HasPrefix(index, "key") && allDigits(strings.TrimPrefix(index, "key")) {
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
