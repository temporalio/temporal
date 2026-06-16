package ownershipcheck_test

import (
	"os"
	"testing"

	"go.temporal.io/server/tools/ownershipcheck"
	"golang.org/x/tools/go/analysis/analysistest"
)

// TestMain supplies the protobuf configuration for the suite — the analyzer has no
// defaults, so sinks/sanitizers must be set explicitly (as the Makefile does for
// CI). Tests that exercise other configs override -sink and restore it here.
func TestMain(m *testing.M) {
	_ = ownershipcheck.Analyzer.Flags.Set("sink", "ProtoReflect")
	_ = ownershipcheck.Analyzer.Flags.Set("sanitizers",
		"maps.Clone,slices.Clone,proto.Clone,proto.CloneOf,common.CloneProto")
	_ = ownershipcheck.Analyzer.Flags.Set("value-kinds", "map,slice")
	os.Exit(m.Run())
}

// TestDirectEmbed covers the intra-procedural sink: a map/slice read directly
// from the receiver and embedded into a returned proto, plus the owned negatives.
func TestDirectEmbed(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer,
		"directembed", // positive: must report
		"freshsend",   // negative: freshly made, never published
		"cloned",      // negative: maps.Clone sanitizes
	)
}

// TestAccessorInference covers return-field inference exported as facts, including
// cross-package resolution. Mirrors the scheduler (#10706) and nexus (#10707) leaks.
func TestAccessorInference(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer,
		"samepackageaccessor",  // positive: same-package accessor inference
		"accessorlib",          // dependency: exports borrowed/owned result facts
		"crosspackageaccessor", // positive: cross-package facts
	)
}

// TestProjection covers provenance-aware projection: a getter on an owned receiver
// (parameter) stays silent, while a value rooted at the shared receiver — even
// through generics, comma-ok type assertions, and multi-level delegation — is
// flagged. Mirrors the chasm.Field[T].Get scheduler case.
func TestProjection(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer,
		"genericfield", // positive: receiver-rooted leak through generic delegation
		"getterparam",  // negative: getter on a parameter receiver stays silent
		"subslice",     // positive: re-slice of a borrowed slice
	)
}

// TestDirectives covers the //ownership: directives: result annotations (override
// inference both ways) and the //ownership:ignore hatch (reason required).
func TestDirectives(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer,
		"annotations",     // //ownership:result owned suppresses; borrowed forces
		"ignoredirective", // //ownership:ignore <reason> suppresses; bare does not
		"paramescapes",    // //ownership:escapes on a param flags borrowed args
		"namedresult",     // //ownership:result <name> targets one of several results
	)
}

// TestEscape covers escape verification (only protos that actually escape the
// function are sinks) and the field-assignment sink (resp.Field = borrowed).
func TestEscape(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer,
		"escapeclone", // negative: build-then-clone, alias never escapes
		"escapelocal", // negative: temp proto consumed locally, never returned
		"fieldassign", // positive: resp.Field = borrowed on a returned proto
		"outputparam", // positive: borrowed written into a proto-pointer parameter
		"namedreturn", // positive: borrowed written into a naked-returned named result
	)
}

// TestInterproc covers interprocedural param summaries: a borrowed value passed to
// a parameter the callee embeds into an escaping proto is flagged at the call site.
func TestInterproc(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer, "builderparam")
}

// TestFlow covers flow-sensitive ownership in both phases: at a sink
// (conditionalclone) and in signature inference of a conditionally-cloned result
// (flowresult). A value cloned on only some paths is borrowed at the merge.
func TestFlow(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer,
		"conditionalclone", "flowresult")
}

// TestGenericSink proves the checker is domain-agnostic: configured with a
// non-proto sink (marker method "Sealed"), it flags embeds into that type and
// nothing proto. The proto behavior is just the default configuration.
func TestGenericSink(t *testing.T) {
	if err := ownershipcheck.Analyzer.Flags.Set("sink", "Sealed"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ownershipcheck.Analyzer.Flags.Set("sink", "ProtoReflect") })
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer, "genericsink")
}

// TestInterfaceSink covers a qualified -sink: the sink is matched by interface
// implementation (types.Implements), so a same-named method with the wrong
// signature does not match.
func TestInterfaceSink(t *testing.T) {
	if err := ownershipcheck.Analyzer.Flags.Set("sink", "ifacesink.Sealer"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ownershipcheck.Analyzer.Flags.Set("sink", "ProtoReflect") })
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer, "ifacesink")
}

// TestReachesLeaf covers noise reduction A: with pointer tracking on, a borrowed
// pointer is flagged only if its pointee transitively reaches a map/slice leaf; a
// pointer to a scalar-only message is silent.
func TestReachesLeaf(t *testing.T) {
	if err := ownershipcheck.Analyzer.Flags.Set("value-kinds", "map,slice,pointer"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ownershipcheck.Analyzer.Flags.Set("value-kinds", "map,slice") })
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer, "reachesleaf")
}

// TestByteSlice covers noise reduction B: a borrowed []byte is a blob/token and is
// not flagged, while a borrowed non-byte slice still is.
func TestByteSlice(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer, "byteslice")
}

// TestInputSet covers noise reduction C: a helper returning one of several inputs
// resolves to owned when its actual arguments are owned (the false positive), yet
// stays borrowed when any argument is borrowed.
func TestInputSet(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer, "inputset")
}

// TestEscapeFuncs covers -escape-funcs: an opaque callee declared to retain arg 0
// makes a borrowed argument at that position a finding, while an owned one is silent.
func TestEscapeFuncs(t *testing.T) {
	if err := ownershipcheck.Analyzer.Flags.Set("escape-funcs", "escapefuncslib.Retain#0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ownershipcheck.Analyzer.Flags.Set("escape-funcs", "") })
	analysistest.Run(t, analysistest.TestData(), ownershipcheck.Analyzer, "escapefuncs")
}

// TestKnownGaps documents false-positive / false-negative cases the analyzer does
// NOT handle yet. It is skipped on purpose: these fixtures (under
// testdata/src/knowngaps) describe desired-but-unimplemented behavior and would
// fail today. See testdata/src/knowngaps/README.md and plan.md. When a gap is
// implemented, move its package into the relevant real test.
func TestKnownGaps(t *testing.T) {
	t.Skip("documents known FP/FN gaps; see testdata/src/knowngaps/README.md and plan.md")
}
