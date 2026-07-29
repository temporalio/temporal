package objectleak

import (
	"reflect"
	"slices"
	"strings"
)

type pattern struct {
	value   string
	matched bool
}

type patterns []pattern

func newPattern(value string) pattern {
	return pattern{value: value}
}

func (p pattern) String() string {
	return p.value
}

func (p pattern) matches(value string) bool {
	if prefix, ok := strings.CutSuffix(p.value, "*"); ok {
		return strings.HasPrefix(value, prefix)
	}
	return value == p.value
}

func (p pattern) matchesObject(obj trackedObject) bool {
	return p.matches(obj.path.normalized()) || p.matches(obj.typeName)
}

func (ps patterns) matchObject(obj trackedObject) []string {
	var matches []string
	for i := range ps {
		if ps[i].matchesObject(obj) {
			ps[i].matched = true
			matches = append(matches, ps[i].String())
		}
	}
	return matches
}

func (ps patterns) matchAny(values ...string) bool {
	for i := range ps {
		if slices.ContainsFunc(values, ps[i].matches) {
			ps[i].matched = true
			return true
		}
	}
	return false
}

func (ps patterns) matchType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Name() != "" {
		return ps.matchAny(t.PkgPath() + "." + t.Name())
	}
	return ps.matchAny(t.String())
}

func (ps patterns) unmatched() []string {
	var unmatched []string
	for _, pattern := range ps {
		if !pattern.matched {
			unmatched = append(unmatched, pattern.String())
		}
	}
	return unmatched
}
