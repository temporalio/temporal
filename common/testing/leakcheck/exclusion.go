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
	path := normalizePathIndexes(obj.path)
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
