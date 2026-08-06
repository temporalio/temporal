package testcore

import (
	"errors"
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/parallelsuite"
)

type runnerContextParallelSuite struct {
	parallelsuite.Suite[*runnerContextParallelSuite]
}

type runnerContextObservations struct {
	mu      sync.Mutex
	names   []string
	routers []*clusterRouter
}

func (o *runnerContextObservations) add(t *testing.T) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.names = append(o.names, LogicalTestName(t))
	o.routers = append(o.routers, routerFor(t))
}

func (s *runnerContextParallelSuite) TestFirst(o *runnerContextObservations) {
	o.add(s.T())
}

func (s *runnerContextParallelSuite) TestSecond(o *runnerContextObservations) {
	o.add(s.T())
}

type teardownRecordingCluster struct {
	Cluster
	teardown func()
	err      error
}

func (c teardownRecordingCluster) TearDownCluster() error {
	c.teardown()
	return c.err
}

func TestLogicalTestName_Default(t *testing.T) {
	require.Equal(t, t.Name(), LogicalTestName(t))
}

func TestLogicalTestName_Runner(t *testing.T) {
	t.Run("imported", func(t *testing.T) {
		Run(t, NewClusterFactory(), func() {
			t.Run("suite", func(t *testing.T) {
				require.Equal(t, "suite", LogicalTestName(t))
			})
		})
	})
}

const duplicateRunContextEnv = "TESTCORE_DUPLICATE_RUN_CONTEXT"

func TestRunContext_DuplicatePhysicalRootFails(t *testing.T) {
	if os.Getenv(duplicateRunContextEnv) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRunContext_DuplicatePhysicalRootFails$")
		cmd.Env = append(os.Environ(), duplicateRunContextEnv+"=1")
		output, err := cmd.CombinedOutput()
		require.Error(t, err)
		require.Contains(t, string(output), `testcore.Run already active for physical root "TestRunContext_DuplicatePhysicalRootFails"`)
		return
	}

	Run(t, NewClusterFactory(), func() {
		Run(t, NewClusterFactory(), func() {
			t.Fatal("duplicate testcore.Run should fail before invoking its callback")
		})
	})
}

func TestRunContext_ParallelSuiteUsesLogicalNamesAndRouter(t *testing.T) {
	observations := &runnerContextObservations{}
	var runnerRouter *clusterRouter
	t.Cleanup(func() {
		require.ElementsMatch(t, []string{
			"TestFirst",
			"TestSecond",
		}, observations.names)
		require.Len(t, observations.routers, 2)
		for _, router := range observations.routers {
			require.Same(t, runnerRouter, router)
		}
	})

	t.Run("imported", func(t *testing.T) {
		Run(t, NewClusterFactory(), func() {
			runnerRouter = routerFor(t)
			parallelsuite.Run(t, &runnerContextParallelSuite{}, observations)
		})
	})
}

func TestRunContext_SequentialSubtestsUseDistinctRoots(t *testing.T) {
	routers := make(map[string]*clusterRouter)
	names := make(map[string]string)

	for _, runName := range []string{"first", "second"} {
		t.Run(runName, func(t *testing.T) {
			Run(t, NewClusterFactory(), func() {
				routers[runName] = routerFor(t)
				t.Run("suite", func(t *testing.T) {
					names[runName] = LogicalTestName(t)
				})
			})
		})
	}

	require.Equal(t, "suite", names["first"])
	require.Equal(t, "suite", names["second"])
	require.NotSame(t, routers["first"], routers["second"])
}

func TestRunContext_DeepestContextDefinesLogicalName(t *testing.T) {
	var outerRouter, innerRouter *clusterRouter
	t.Run("outer", func(t *testing.T) {
		Run(t, NewClusterFactory(), func() {
			outerRouter = routerFor(t)
			t.Run("inner", func(t *testing.T) {
				Run(t, NewClusterFactory(), func() {
					innerRouter = routerFor(t)
					t.Run("suite", func(t *testing.T) {
						require.Equal(t, "suite", LogicalTestName(t))
						require.Same(t, innerRouter, routerFor(t))
					})
				})
			})
			t.Run("outer-suite", func(t *testing.T) {
				require.Equal(t, "outer-suite", LogicalTestName(t))
				require.Same(t, outerRouter, routerFor(t))
			})
		})
	})
	require.NotSame(t, outerRouter, innerRouter)
}

func TestRunContext_SuiteScopedClusterRoutesBelowNestedParent(t *testing.T) {
	t.Run("imported", func(t *testing.T) {
		Run(t, NewClusterFactory(), func() {
			t.Run("nested", func(t *testing.T) {
				UseSuiteScopedCluster(t)
				t.Run("method", func(t *testing.T) {
					t.Parallel()
					require.True(t, routerFor(t).hasSuiteScoped(t))
				})
			})
		})
	})
}

func TestRunContext_CleanupWaitsForParallelDescendants(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var events []string

	go func() {
		<-started
		close(release)
	}()

	t.Run("imported", func(t *testing.T) {
		Run(t, NewClusterFactory(), func() {
			t.Run("suite", func(t *testing.T) {
				UseSuiteScopedCluster(t)
				suiteCluster := routerFor(t).suiteScopedFor(t)
				suiteCluster.mu.Lock()
				suiteCluster.cluster = &FunctionalTestBase{
					testCluster: teardownRecordingCluster{
						teardown: func() { events = append(events, "teardown") },
					},
				}
				suiteCluster.mu.Unlock()
				t.Run("parallel", func(t *testing.T) {
					t.Parallel()
					close(started)
					<-release
					events = append(events, "descendant")
				})
			})
		})
	})

	require.Equal(t, []string{"descendant", "teardown"}, events)
}

func TestRunContext_RouterClosePropagatesEveryTearDownError(t *testing.T) {
	sharedErr := errors.New("shared cluster teardown failed")
	dedicatedErr := errors.New("dedicated cluster teardown failed")
	suiteErr := errors.New("suite cluster teardown failed")
	var calls []string
	router := newClusterRouter(NewClusterFactory(), clusterRouterConfig{
		sharedSize:    1,
		dedicatedSize: 1,
	})
	router.shared.allSlots[0].cluster = &FunctionalTestBase{
		testCluster: teardownRecordingCluster{
			teardown: func() { calls = append(calls, "shared") },
			err:      sharedErr,
		},
	}
	router.dedicated.allSlots[0].cluster = &FunctionalTestBase{
		testCluster: teardownRecordingCluster{
			teardown: func() { calls = append(calls, "dedicated") },
			err:      dedicatedErr,
		},
	}
	router.suiteScoped.Store("suite", &suiteScopedCluster{
		cluster: &FunctionalTestBase{
			testCluster: teardownRecordingCluster{
				teardown: func() { calls = append(calls, "suite") },
				err:      suiteErr,
			},
		},
	})

	err := router.close()

	require.ErrorIs(t, err, sharedErr)
	require.ErrorIs(t, err, dedicatedErr)
	require.ErrorIs(t, err, suiteErr)
	require.Equal(t, []string{"shared", "dedicated", "suite"}, calls)
	require.Nil(t, router.shared.allSlots[0].cluster)
	require.Nil(t, router.dedicated.allSlots[0].cluster)
	_, ok := router.suiteScoped.Load("suite")
	require.False(t, ok)
}

func TestRunContext_PreservesShardOwner(t *testing.T) {
	const total = 3
	var logicalName string

	t.Run("imported", func(t *testing.T) {
		Run(t, NewClusterFactory(), func() {
			t.Run("suite", func(t *testing.T) {
				logicalName = LogicalTestName(t)
			})
		})
	})

	require.Equal(t, testShardOwner("suite", total), testShardOwner(logicalName, total))
}

func TestRunContext_RecordsShardDecisions(t *testing.T) {
	var ctx *RunContext
	Run(t, NewClusterFactory(), func() {
		ctx = runContextFor(t)
		t.Run("suite", func(t *testing.T) {
			checkTestShard(t)
		})
	})

	require.Equal(t, []ShardRecord{{
		LogicalName: "suite",
		Owner:       testShardOwner("suite", 1),
		Total:       1,
		Owned:       true,
	}}, ctx.shards())
}
