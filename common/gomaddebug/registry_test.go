package gomaddebug

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestRegistryDeclarationAndClosure(t *testing.T) {
	var r registry
	id := r.declare(kindProbe, "workflow.event", 0, nil, nil)
	if id != 0 || r.closed || r.failed {
		t.Fatal("dormant registry lost declaration")
	}
	if got := r.declare(kindCheck, "workflow.valid", 1, nil, func() bool { return true }); got != 1 || r.entries[got].flags != 1 {
		t.Fatal("declaration order/monotonicity lost")
	}
	r.close()
	if !r.closed || r.failed {
		t.Fatal("registry did not close")
	}
	defer func() {
		if recover() == nil || !r.failed {
			t.Fatal("late registration must poison registry")
		}
	}()
	r.declare(kindProbe, "late", 0, nil, nil)
}

func TestRegistryRefusesIncomplete(t *testing.T) {
	for _, which := range []string{"duplicate", "capacity", "name", "callback"} {
		t.Run(which, func(t *testing.T) {
			var r registry
			defer func() {
				if recover() == nil || !r.failed {
					t.Fatal("invalid declaration did not poison registration")
				}
			}()
			switch which {
			case "duplicate":
				r.declare(kindProbe, "same", 0, nil, nil)
				r.declare(kindDump, "same", 0, func(*DumpWriter) {}, nil)
			case "capacity":
				r.count = maxEntries
				r.declare(kindProbe, "extra", 0, nil, nil)
			case "name":
				r.declare(kindProbe, strings.Repeat("a", maxName+1), 0, nil, nil)
			case "callback":
				r.declare(kindDump, "nil", 0, nil, nil)
			}
		})
	}
}

func TestTypedProbeEncodingAndAllocations(t *testing.T) {
	var r registry
	var captured [emitCapacity]byte
	n := 0
	r.send = func(p []byte) { n = copy(captured[:], p) }
	fields := []Field{Int64(-7), Uint64(9), Bool(true), Float64(1.5), String("existing")}
	if allocs := testing.AllocsPerRun(100, func() { r.emit(2, fields) }); allocs != 0 {
		t.Fatalf("allocations: %v", allocs)
	}
	if allocs := testing.AllocsPerRun(100, func() { r.emit(2, []Field{Int64(-7), Uint64(9), Bool(true), Float64(1.5), String("existing")}) }); allocs != 0 {
		t.Fatalf("field construction allocations: %v", allocs)
	}
	if n != 16+5*5+8+8+1+8+8 || binary.LittleEndian.Uint32(captured[:4]) != 1 || binary.LittleEndian.Uint32(captured[4:8]) != 2 || binary.LittleEndian.Uint32(captured[8:12]) != 5 {
		t.Fatalf("bad typed record: %x", captured[:n])
	}
	if got := int64(binary.LittleEndian.Uint64(captured[21:29])); got != -7 {
		t.Fatalf("signed field = %d", got)
	}
	if string(captured[n-8:n]) != "existing" {
		t.Fatal("string bytes lost")
	}
	large := []Field{String(strings.Repeat("x", emitCapacity))}
	if allocs := testing.AllocsPerRun(100, func() { r.emit(2, large) }); allocs != 0 {
		t.Fatalf("drop allocations: %v", allocs)
	}
	if binary.LittleEndian.Uint32(captured[:4]) != 4 || r.dropped != 101 {
		t.Fatal("missing counted overflow marker")
	}
}

func TestDumpIsolationAndInvariantPanic(t *testing.T) {
	var r registry
	var statuses []uint32
	r.send = func(p []byte) { statuses = append(statuses, binary.LittleEndian.Uint32(p[8:12])) }
	r.declare(kindDump, "panic", 0, func(*DumpWriter) { panic("fixture") }, nil)
	r.declare(kindDump, "large", 0, func(w *DumpWriter) { w.String(strings.Repeat("x", dumpCapacity+1)) }, nil)
	r.declare(kindDump, "good", 0, func(w *DumpWriter) { w.String("good") }, nil)
	r.declare(kindCheck, "check", 1, nil, func() bool { panic("fixture") })
	for i := uint32(0); i < 4; i++ {
		r.inspect(r.entries[i].kind, i)
	}
	for i, want := range []uint32{1, 2, 0, 2} {
		if statuses[i] != want {
			t.Fatalf("status[%d]=%d want %d", i, statuses[i], want)
		}
	}
}
