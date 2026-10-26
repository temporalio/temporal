// Package gomaddebug declares dormant replay diagnostics. Calls are inert outside
// the cooperative Gomad WASM runtime. Guard probe field evaluation with Enabled.
package gomaddebug

import "math"

type Probe struct{ id uint8 }

// Field holds a primitive value or an existing string without boxing.
type Field struct {
	kind byte
	bits uint64
	text string
}

func Int64(v int64) Field   { return Field{kind: 1, bits: uint64(v)} }
func Uint64(v uint64) Field { return Field{kind: 2, bits: v} }
func Bool(v bool) Field {
	var bits uint64
	if v {
		bits = 1
	}
	return Field{kind: 3, bits: bits}
}
func Float64(v float64) Field { return Field{kind: 4, bits: math.Float64bits(v)} }
func String(v string) Field   { return Field{kind: 5, text: v} }

// DumpWriter bounds a single callback's output. Writes past the bound are
// discarded and reported as truncation; callbacks must not wait on application
// goroutines or acquire application locks, which remain frozen during inspection.
type DumpWriter struct {
	bytes     [dumpCapacity]byte
	n         int
	truncated bool
}

func (w *DumpWriter) Write(p []byte) (int, error) {
	n := copy(w.bytes[w.n:], p)
	w.n += n
	w.truncated = w.truncated || n != len(p)
	return len(p), nil
}

func (w *DumpWriter) String(s string) {
	n := copy(w.bytes[w.n:], s)
	w.n += n
	w.truncated = w.truncated || n != len(s)
}
