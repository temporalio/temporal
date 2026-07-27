package objectleak

import (
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
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
}

func TestObjectLeak_Check(t *testing.T) {
	for _, tc := range []struct {
		name            string
		opts            []Option
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
retained paths: 6 total, 0 expected, 6 unexpected
retained objects: 4 total, 0 expected, 4 unexpected

unexpected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)
  2 paths, 1 object: Node.Leaf (*objectleak.graphLeaf)

expected retained objects:
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
retained paths: 6 total, 6 expected, 0 unexpected
retained objects: 4 total, 4 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)
  2 paths, 1 object: Node.Leaf (*objectleak.graphLeaf)`,
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
retained paths: 6 total, 6 expected, 0 unexpected
retained objects: 4 total, 4 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)
  2 paths, 1 object: Node.Leaf (*objectleak.graphLeaf)

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
retained paths: 6 total, 6 expected, 0 unexpected
retained objects: 4 total, 4 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)
  2 paths, 1 object: Node.Leaf (*objectleak.graphLeaf)

stale prunes:
  does.not.Match`,
		},
		{
			name: "prunes matched type",
			opts: []Option{
				WithExpected("*objectleak.graphRoot"),
				WithExpected("*objectleak.graphNode"),
				WithPruneType(reflect.TypeFor[graphNode]().PkgPath() + ".graphNode"),
			},
			wantReport: `object leak report

tracked root objects: 2
retained paths: 4 total, 4 expected, 0 unexpected
retained objects: 3 total, 3 expected, 0 unexpected

unexpected retained objects:
  none

expected retained objects:
  2 objects: *objectleak.graphRoot
  2 paths, 1 object: Node (*objectleak.graphNode)`,
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
			opts = append(opts, WithGCSettleTimeout(10*time.Millisecond))

			check, err := NewObjectLeakCheck(opts...)
			require.NoError(t, err)
			for _, root := range roots {
				check.Track(root)
			}

			report, err := check.Check()
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

func TestObjectLeak_IgnoreCurrent(t *testing.T) {
	sharedLeaf := &graphLeaf{Value: 2}
	baselineRoot := &graphRoot{
		Node: &graphNode{Leaf: sharedLeaf},
	}

	check, err := NewObjectLeakCheck(WithGCSettleTimeout(10 * time.Millisecond))
	require.NoError(t, err)
	check.Track(baselineRoot)
	baselineRoot = nil
	check.IgnoreCurrent()

	root := &graphRoot{
		Node: &graphNode{Leaf: sharedLeaf},
	}
	check.Track(root)

	report, err := check.Check()
	runtime.KeepAlive(root)
	require.Error(t, err)
	require.Equal(t, `object leak report

tracked root objects: 1
retained paths: 3 total, 1 baseline, 0 expected, 2 unexpected
retained objects: 3 total, 1 baseline, 0 expected, 2 unexpected

unexpected retained objects:
  1 object: *objectleak.graphRoot
  1 object: Node (*objectleak.graphNode)

expected retained objects:
  none

baseline retained objects:
  1 object: Node.Leaf (*objectleak.graphLeaf)`, report)
}

func TestObjectLeak_IgnoreCurrentObjectCollectedAfterSnapshot(t *testing.T) {
	root := &graphRoot{}
	check, err := NewObjectLeakCheck(WithGCSettleTimeout(10 * time.Millisecond))
	require.NoError(t, err)
	check.Track(root)
	check.IgnoreCurrent()
	runtime.KeepAlive(root)

	require.Len(t, check.baselineByID, 1)
	var identity objectIdentity
	var baseline trackedObject
	for identity, baseline = range check.baselineByID {
	}

	root = nil
	await.RequireTrue(t, func() bool {
		runtime.GC()
		return baseline.collected.Load()
	}, time.Second, 10*time.Millisecond)
	require.Nil(t, check.baselineCollected(trackedObject{
		addr:     identity.addr,
		typeName: identity.typeName,
	}))
}

func TestObjectLeak_IgnoreCurrentCollectionAfterTrack(t *testing.T) {
	baselineCollected := &atomic.Bool{}
	obj := trackedObject{
		addr:              1,
		typeName:          "*objectleak.graphRoot",
		baselineCollected: baselineCollected,
		collected:         &atomic.Bool{},
	}
	baselineCollected.Store(true)

	report := newReport([]trackedObject{obj}, 1, nil, nil)
	require.Zero(t, report.baselineRetainedObjects)
	require.Equal(t, 1, report.unexpectedRetainedObjects)
}
