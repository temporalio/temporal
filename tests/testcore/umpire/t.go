package umpire

import (
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// T is the test context for property-based test actions.
type T struct {
	*require.Assertions
	raw      *rapid.T
	coverage *coverageCollector
}

func newT(t *rapid.T, coverage ...*coverageCollector) *T {
	mt := &T{raw: t}
	if len(coverage) != 0 {
		mt.coverage = coverage[0]
	}
	mt.Assertions = require.New(mt)
	return mt
}

func (t *T) Helper() {
	t.raw.Helper()
}

func (t *T) Skip(args ...any) {
	t.raw.Skip(args...)
}

func (t *T) Skipf(format string, args ...any) {
	t.raw.Skipf(format, args...)
}

func (t *T) Logf(format string, args ...any) {
	t.Helper()
	t.raw.Logf("[umpire] "+format, args...)
}

func (t *T) Errorf(format string, args ...any) {
	t.raw.Errorf(format, args...)
}

func (t *T) FailNow() {
	t.raw.FailNow()
}

func (t *T) Fatalf(format string, args ...any) {
	t.raw.Fatalf(format, args...)
}
