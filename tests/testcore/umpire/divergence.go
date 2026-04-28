package umpire

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// TraceHash accumulates a stable fingerprint of a sequence of observed RPC
// calls. Two runs driven by the same rapid seed should produce an identical
// hash once nondeterministic fields are canonicalized away; a mismatch
// pinpoints a nondeterminism source (or a real SUT bug). This is a diagnostic
// — it surfaces divergence, it does not prevent it (see plan.L0-divergence.md).
//
// One TraceHash holds one trace's running hash plus the idRewriter that keeps
// its first-seen index assignments stable; Reset clears both between traces.
type TraceHash struct {
	mu    sync.Mutex
	h     hash.Hash64
	rw    *idRewriter
	n     int
	lines []string
}

// NewTraceHash returns an empty TraceHash ready to fold observed calls.
func NewTraceHash() *TraceHash {
	return &TraceHash{h: fnv.New64a(), rw: newIDRewriter()}
}

// Sum64 returns the current fingerprint of the folded calls.
func (t *TraceHash) Sum64() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.h.Sum64()
}

// Count returns the number of calls folded since the last Reset.
func (t *TraceHash) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.n
}

// Reset clears the hash, the first-seen index table, and the captured call
// lines, readying the TraceHash for the next trace (e.g. the next rapid
// iteration).
func (t *TraceHash) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.h.Reset()
	t.rw = newIDRewriter()
	t.n = 0
	t.lines = nil
}

// Calls returns the canonical, nondeterminism-stripped form of each folded
// call in order. Diff two same-seed traces' Calls (see FirstDivergence) to
// pinpoint where their behavior diverged.
func (t *TraceHash) Calls() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.lines))
	copy(out, t.lines)
	return out
}

func (t *TraceHash) fold(method string, req, resp any, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	reqC := canonicalAny(req, t.rw)
	respC := canonicalAny(resp, t.rw)
	errC := canonicalErr(err)
	t.write(method)
	t.write(reqC)
	t.write(respC)
	t.write(errC)
	t.lines = append(t.lines, method+" | req="+reqC+" | resp="+respC+" | err="+errC)
	t.n++
}

// FoldHash folds another fingerprint into this one. Use it to combine
// per-iteration trace hashes into a single per-run fingerprint that can be
// logged once outside the rapid property closure — where logs survive a
// passing run — and compared across same-seed runs.
func (t *TraceHash) FoldHash(sum uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], sum)
	_, _ = t.h.Write(buf[:])
	t.n++
}

func (t *TraceHash) write(s string) {
	_, _ = t.h.Write([]byte(s))
	_, _ = t.h.Write([]byte{0}) // field separator to avoid concatenation collisions
}

func canonicalAny(v any, rw *idRewriter) string {
	if v == nil {
		return "<nil>"
	}
	if m, ok := v.(proto.Message); ok {
		return string(canonicalProtoBytes(m, rw))
	}
	return fmt.Sprintf("%T:%v", v, v)
}

// FirstDivergence returns the index of the first call where traces a and b
// differ, or -1 if one is a prefix of the other and all shared calls match.
// When two same-seed runs log different fingerprints, feed their captured
// Calls here to find the exact diverging RPC.
func FirstDivergence(a, b []string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}

// DivergenceStrategy folds a canonical fingerprint of every client-side call
// into out. Client-side only (like ObserveStrategy) so calls made through the
// test client are counted exactly once. Register out with NewTraceHash and
// Reset it at the start of each trace.
func DivergenceStrategy(out *TraceHash) Strategy {
	return Strategy{
		Name: "divergence",
		Client: func(
			ctx context.Context,
			method string,
			req, reply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			opts ...grpc.CallOption,
		) error {
			err := invoker(ctx, method, req, reply, cc, opts...)
			out.fold(method, req, reply, err)
			return err
		},
	}
}
