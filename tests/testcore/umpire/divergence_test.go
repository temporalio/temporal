package umpire

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// canonicalProtoBytes must ignore nondeterministic fields (run_id) but remain
// sensitive to meaningful ones (workflow_id). This is the load-bearing L0.5
// property: same seed -> same canonical form regardless of server-assigned IDs.
func TestCanonicalProtoBytes_NormalizesNondeterministicFields(t *testing.T) {
	a := &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-aaaa"}
	b := &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-bbbb"}

	// Differ only in run_id -> identical canonical form (each rewriter assigns
	// its first-seen run_id index #0).
	require.Equal(t,
		string(canonicalProtoBytes(a, newIDRewriter())),
		string(canonicalProtoBytes(b, newIDRewriter())),
		"messages differing only in run_id should canonicalize identically",
	)

	// Differ in a meaningful field -> distinct canonical form.
	c := &commonpb.WorkflowExecution{WorkflowId: "wf-2", RunId: "run-aaaa"}
	require.NotEqual(t,
		string(canonicalProtoBytes(a, newIDRewriter())),
		string(canonicalProtoBytes(c, newIDRewriter())),
		"messages differing in workflow_id should canonicalize differently",
	)
}

// Within one trace, the same logical nondeterministic value must map to the
// same index, and distinct values to distinct indices, so structure is
// preserved while content is erased.
func TestIDRewriter_StableFirstSeenIndices(t *testing.T) {
	rw := newIDRewriter()
	require.Equal(t, "#0", rw.rewrite("alpha"))
	require.Equal(t, "#1", rw.rewrite("beta"))
	require.Equal(t, "#0", rw.rewrite("alpha"), "repeat value reuses its index")
	require.Equal(t, "#2", rw.rewrite("gamma"))
}

// Two traces that differ only in server-assigned IDs (in the same positions)
// must hash equal; a difference in a meaningful field must diverge.
func TestTraceHash_SameSeedShapeConverges(t *testing.T) {
	const method = "/temporal.api.workflowservice.v1.WorkflowService/DescribeNexusOperationExecution"

	run := func(runID, wfID string) uint64 {
		h := NewTraceHash()
		h.fold(method, &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: runID}, nil, nil)
		h.fold(method, &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: runID}, nil, nil)
		return h.Sum64()
	}

	// Same shape, different run IDs -> equal hashes.
	require.Equal(t, run("run-aaaa", "wf-1"), run("run-zzzz", "wf-1"),
		"traces differing only in run_id should hash equal")

	// Different meaningful field -> divergent hash (the mechanism bites).
	require.NotEqual(t, run("run-aaaa", "wf-1"), run("run-aaaa", "wf-2"),
		"traces differing in workflow_id should diverge")
}

func TestTraceHash_ResetClearsState(t *testing.T) {
	h := NewTraceHash()
	empty := h.Sum64()
	h.fold("/m", &commonpb.WorkflowExecution{WorkflowId: "wf"}, nil, nil)
	require.Equal(t, 1, h.Count())
	require.NotEqual(t, empty, h.Sum64())

	h.Reset()
	require.Equal(t, 0, h.Count())
	require.Equal(t, empty, h.Sum64(), "Reset should return the hash to its empty state")
}

func TestTraceHash_CapturesCanonicalCalls(t *testing.T) {
	const method = "/svc/Method"
	h := NewTraceHash()
	h.fold(method, &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run-1"}, nil, nil)
	calls := h.Calls()
	require.Len(t, calls, 1)
	require.Contains(t, calls[0], method)
	require.Contains(t, calls[0], "wf")
	require.NotContains(t, calls[0], "run-1", "run_id should be normalized away in the captured line")

	h.Reset()
	require.Empty(t, h.Calls(), "Reset clears captured calls")
}

func TestFirstDivergence(t *testing.T) {
	require.Equal(t, -1, FirstDivergence(nil, nil))
	require.Equal(t, -1, FirstDivergence([]string{"a", "b"}, []string{"a", "b"}))
	require.Equal(t, 1, FirstDivergence([]string{"a", "b"}, []string{"a", "c"}))
	// One is a prefix of the other -> first extra element is the divergence.
	require.Equal(t, 2, FirstDivergence([]string{"a", "b"}, []string{"a", "b", "c"}))
	require.Equal(t, 0, FirstDivergence([]string{"x"}, []string{"y", "z"}))
}

func TestCanonicalErr(t *testing.T) {
	require.Equal(t, "<nil>", canonicalErr(nil))

	// gRPC code is preserved; embedded UUID and timestamp are stripped so
	// incidental identifiers don't cause false divergence.
	err := status.Errorf(codes.NotFound,
		"operation 11111111-2222-3333-4444-555555555555 at 2026-05-31T12:00:00Z not found")
	got := canonicalErr(err)
	require.Contains(t, got, codes.NotFound.String())
	require.Contains(t, got, "<uuid>")
	require.Contains(t, got, "<time>")
	require.NotContains(t, got, "11111111-2222-3333-4444-555555555555")

	// Same shape, different incidental IDs -> identical canonical form.
	err2 := status.Errorf(codes.NotFound,
		"operation 99999999-8888-7777-6666-555555555555 at 2027-01-01T00:00:00Z not found")
	require.Equal(t, got, canonicalErr(err2))

	require.NotEqual(t, got, canonicalErr(errors.New("totally different")))
}
