package objectleak

import (
	"reflect"
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

func (p pattern) matchesObject(path string, typeName string) bool {
	return p.matches(path) || p.matches(typeName)
}

func (ps patterns) matchObject(path string, typeName string) bool {
	matched := false
	for i := range ps {
		if ps[i].matchesObject(path, typeName) {
			ps[i].matched = true
			matched = true
		}
	}
	return matched
}

func (ps patterns) matchValue(value string) bool {
	matched := false
	for i := range ps {
		if ps[i].matches(value) {
			ps[i].matched = true
			matched = true
		}
	}
	return matched
}

func (ps patterns) matchType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Name() != "" {
		return ps.matchValue(t.PkgPath() + "." + t.Name())
	}
	return ps.matchValue(t.String())
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
