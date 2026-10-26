//go:build !gomad || !wasip1 || !wasm

package gomaddebug

import "testing"

func TestNoopGuardSkipsFields(t *testing.T) {
	p := DeclareProbe("noop")
	called := false
	if p.Enabled() {
		called = true
		p.Emit(String("never"))
	}
	RegisterDump("noop.dump", func(*DumpWriter) { called = true })
	RegisterInvariant("noop.check", true, func() bool { called = true; return false })
	if called {
		t.Fatal("ordinary/native build executed debug code")
	}
	if allocs := testing.AllocsPerRun(100, func() {
		p := DeclareProbe("noop")
		if p.Enabled() {
			p.Emit(Int64(1), String("existing"))
		}
	}); allocs != 0 {
		t.Fatalf("noop allocations: %v", allocs)
	}
}
