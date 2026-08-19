package objectleak

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testGCSettleTimeout = 100 * time.Millisecond
	testGCSettleMinWait = 5 * time.Millisecond
	testGCSettleQuiet   = 5 * time.Millisecond
)

type graphRoot struct {
	Node *graphNode
}

type graphNode struct {
	Leaf  *graphLeaf
	Value int
}

type graphLeaf struct {
	Value int
	_     [8]byte
}

type tinyValue byte

type tinyZeroLengthPointerArrayValue struct {
	Pointers [0]*byte
	Value    byte
}

type aliasOuter struct {
	aliasInner
	_ [8]byte
}

type aliasInner struct {
	Value int
	_     [8]byte
}

func TestObjectLeak_Check(t *testing.T) {
	for _, tc := range []struct {
		name            string
		opts            []Option
		setup           func(*ObjectLeakCheck, []any) Baseline
		gcSettleTimeout time.Duration
		wantErrContains []string
		wantReport      string
	}{
		{
			name: "reports retained objects",
			wantErrContains: []string{
				"unexpected retained objects",
			},
			wantReport: `object leak report

tracked root objects: 2
retained paths: 6 total, 0 baseline, 0 expected, 6 unexpected
retained objects: 4 total, 0 baseline, 0 expected, 4 unexpected

unexpected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)
  2 paths, 1 object: Node.Leaf (*objectleak.graphLeaf)

expected retained objects:
  none

baseline retained objects:
  none`,
		},
		{
			name: "allows expected retained objects",
			opts: []Option{
				WithExpected("*objectleak.graphRoot"),
				WithExpected("*objectleak.graphNode"),
				WithExpected("*objectleak.graphLeaf"),
				WithExpected("Node*"),
			},
			wantReport: `object leak report

tracked root objects: 2
retained paths: 6 total, 0 baseline, 6 expected, 0 unexpected
retained objects: 4 total, 0 baseline, 4 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)
  2 paths, 1 object: Node.Leaf (*objectleak.graphLeaf)

baseline retained objects:
  none`,
		},
		{
			name: "fails stale expected pattern",
			opts: []Option{
				WithExpected("*objectleak.graphRoot"),
				WithExpected("Node*"),
				WithExpected("does.not.match"),
			},
			wantErrContains: []string{
				"stale expected patterns",
			},
			wantReport: `object leak report

tracked root objects: 2
retained paths: 6 total, 0 baseline, 6 expected, 0 unexpected
retained objects: 4 total, 0 baseline, 4 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)
  2 paths, 1 object: Node.Leaf (*objectleak.graphLeaf)

baseline retained objects:
  none

stale expected patterns:
  does.not.match`,
		},
		{
			name: "fails stale prune",
			opts: []Option{
				WithExpected("*objectleak.graphRoot"),
				WithExpected("Node*"),
				WithPruneType("does.not.Match"),
			},
			wantErrContains: []string{
				"stale prunes",
			},
			wantReport: `object leak report

tracked root objects: 2
retained paths: 6 total, 0 baseline, 6 expected, 0 unexpected
retained objects: 4 total, 0 baseline, 4 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)
  2 paths, 1 object: Node.Leaf (*objectleak.graphLeaf)

baseline retained objects:
  none

stale prunes:
  does.not.Match`,
		},
		{
			name: "prunes matched type",
			opts: []Option{
				WithExpected("*objectleak.graphRoot"),
				WithExpected("*objectleak.graphNode"),
				WithPruneType(reflect.TypeFor[graphNode]().PkgPath() + ".graphNode"),
				WithPruneType(reflect.TypeFor[graphNode]().PkgPath() + ".graphN*"),
			},
			wantReport: `object leak report

tracked root objects: 2
retained paths: 4 total, 0 baseline, 4 expected, 0 unexpected
retained objects: 3 total, 0 baseline, 3 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)

baseline retained objects:
  none`,
		},
		{
			name: "reports baseline-only expected patterns stale",
			opts: []Option{
				WithExpected("*objectleak.graphLeaf"),
			},
			setup: func(check *ObjectLeakCheck, roots []any) Baseline {
				check.Track(roots[0])
				baseline := check.IgnoreCurrent()
				check.Track(roots[1]) // shared node and leaf remain baseline-covered
				return baseline
			},
			wantErrContains: []string{
				"unexpected retained objects",
				"stale expected patterns",
			},
			wantReport: `object leak report

tracked root objects: 1
retained paths: 4 total, 3 baseline, 0 expected, 1 unexpected
retained objects: 4 total, 3 baseline, 0 expected, 1 unexpected

unexpected retained objects:
  1 object: *objectleak.graphRoot

expected retained objects:
  none

baseline retained objects:
  1 object: *objectleak.graphRoot
  1 object: Node (*objectleak.graphNode)
  1 object: Node.Leaf (*objectleak.graphLeaf)

stale expected patterns:
  *objectleak.graphLeaf`,
		},
		{
			name:            "reports check GC settle timeout",
			gcSettleTimeout: time.Nanosecond,
			wantErrContains: []string{
				"check GC settling timed out",
				"unexpected retained objects",
			},
			wantReport: `object leak report

tracked root objects: 2
check GC settling timed out
retained paths: 6 total, 0 baseline, 0 expected, 6 unexpected
retained objects: 4 total, 0 baseline, 0 expected, 4 unexpected

unexpected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)
  2 paths, 1 object: Node.Leaf (*objectleak.graphLeaf)

expected retained objects:
  none

baseline retained objects:
  none`,
		},
		{
			name:            "reports baseline GC settle timeout",
			gcSettleTimeout: time.Nanosecond,
			setup: func(check *ObjectLeakCheck, roots []any) Baseline {
				check.Track(roots[0])
				baseline := check.IgnoreCurrent()           // records the baseline settle timeout
				check.gcSettleTimeout = testGCSettleTimeout // lets Check settle normally
				check.Track(roots[1])
				return baseline
			},
			wantErrContains: []string{
				"baseline GC settling timed out",
				"unexpected retained objects",
			},
			wantReport: `object leak report

tracked root objects: 1
baseline GC settling timed out
retained paths: 4 total, 3 baseline, 0 expected, 1 unexpected
retained objects: 4 total, 3 baseline, 0 expected, 1 unexpected

unexpected retained objects:
  1 object: *objectleak.graphRoot

expected retained objects:
  none

baseline retained objects:
  1 object: *objectleak.graphRoot
  1 object: Node (*objectleak.graphNode)
  1 object: Node.Leaf (*objectleak.graphLeaf)`,
		},
		{
			name: "deduplicates baseline objects across roots",
			setup: func(check *ObjectLeakCheck, roots []any) Baseline {
				check.Track(roots[0])
				check.Track(roots[1]) // duplicates the shared node and leaf across Track calls
				baseline := check.IgnoreCurrent()
				check.Track(roots[0]) // all objects remain covered by the deduplicated baseline
				return baseline
			},
			wantReport: `object leak report

tracked root objects: 1
retained paths: 6 total, 6 baseline, 0 expected, 0 unexpected
retained objects: 4 total, 4 baseline, 0 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  none

baseline retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)
  2 paths, 1 object: Node.Leaf (*objectleak.graphLeaf)`,
		},
		{
			name: "matches baseline objects by address",
			setup: func(check *ObjectLeakCheck, roots []any) Baseline {
				outer := &aliasOuter{}
				roots[0] = outer
				check.Track(outer)
				baseline := check.IgnoreCurrent()
				check.Track(&outer.aliasInner) // same address as outer, but a different pointer type
				return baseline
			},
			wantReport: `object leak report

tracked root objects: 1
retained paths: 1 total, 1 baseline, 0 expected, 0 unexpected
retained objects: 1 total, 1 baseline, 0 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  none

baseline retained objects:
  1 object: *objectleak.aliasOuter`,
		},
		{
			name: "drops collected baseline objects",
			setup: func(check *ObjectLeakCheck, roots []any) Baseline {
				check.Track(roots[0])
				baseline := check.IgnoreCurrent()
				roots[0] = nil // allows the baselined root to be collected before Check
				check.Track(roots[1])
				return baseline
			},
			wantErrContains: []string{
				"unexpected retained objects",
			},
			wantReport: `object leak report

tracked root objects: 1
retained paths: 3 total, 2 baseline, 0 expected, 1 unexpected
retained objects: 3 total, 2 baseline, 0 expected, 1 unexpected

unexpected retained objects:
  1 object: *objectleak.graphRoot

expected retained objects:
  none

baseline retained objects:
  1 object: Node (*objectleak.graphNode)
  1 object: Node.Leaf (*objectleak.graphLeaf)`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			node := &graphNode{
				Leaf:  &graphLeaf{Value: 2},
				Value: 1,
			}
			roots := []any{
				&graphRoot{Node: node},
				&graphRoot{Node: node},
			}

			opts := append([]Option{}, tc.opts...)
			gcSettleTimeout := tc.gcSettleTimeout
			if gcSettleTimeout == 0 {
				gcSettleTimeout = testGCSettleTimeout
			}
			opts = append(opts, WithGCSettleTimeout(gcSettleTimeout))

			check, err := NewObjectLeakCheck(opts...)
			require.NoError(t, err)
			check.gcSettleMinWait = testGCSettleMinWait
			check.gcSettleQuiet = testGCSettleQuiet

			var baseline Baseline
			if tc.setup != nil {
				baseline = tc.setup(&check, roots)
			} else {
				for _, root := range roots {
					check.Track(root)
				}
			}

			report, err := check.Check(baseline)
			runtime.KeepAlive(roots) // prevent GC from reclaiming roots

			if len(tc.wantErrContains) > 0 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			for _, expected := range tc.wantErrContains {
				require.Contains(t, err.Error(), expected)
			}
			require.Equal(t, tc.wantReport, report)
		})
	}
}

func TestObjectLeak_CheckSkipsTinyPointerFreeObjects(t *testing.T) {
	values := []any{
		new(tinyValue),
		new(tinyZeroLengthPointerArrayValue),
	}
	check, err := NewObjectLeakCheck(WithGCSettleTimeout(testGCSettleTimeout))
	require.NoError(t, err)
	check.gcSettleMinWait = testGCSettleMinWait
	check.gcSettleQuiet = testGCSettleQuiet
	for _, value := range values {
		check.Track(value)
	}

	report, err := check.Check(Baseline{})
	runtime.KeepAlive(values)
	require.NoError(t, err)
	require.Equal(t, `object leak report

tracked root objects: 2
retained paths: 0 total, 0 baseline, 0 expected, 0 unexpected
retained objects: 0 total, 0 baseline, 0 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  none

baseline retained objects:
  none`, report)
}

func TestSettleGCToZeroSucceedsWhenObjectsDrainNearTimeout(t *testing.T) {
	start := time.Now()
	require.True(t, settleGCToZero(testGCSettleTimeout, testGCSettleMinWait, testGCSettleQuiet, func() int {
		if time.Since(start) < testGCSettleMinWait {
			return 1
		}
		return 0
	}))
}

func TestSettleGCToZeroWaitsForQuietAfterObjectsDrain(t *testing.T) {
	calls := 0
	require.True(t, settleGCToZero(testGCSettleTimeout, testGCSettleMinWait, testGCSettleQuiet, func() int {
		calls++
		if calls == 1 {
			return 1
		}
		return 0
	}))
	require.Greater(t, calls, 2)
}
