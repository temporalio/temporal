package testenv

import (
	"sync"
	"testing"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/tests/acceptance/model"
	"go.temporal.io/server/tests/acceptance/testenv/action"
)

var (
	temporalMdl = stamp.NewModelSet()
)

type (
	suite struct {
		*stamp.Suite
		sharedClusterLock sync.Mutex
		sharedCluster     *physicalCluster // shared across tests that have no custom config
	}
)

func init() {
	stamp.RegisterModel[*model.Cluster](temporalMdl)
	stamp.RegisterModel[*model.Namespace](temporalMdl)
	stamp.RegisterModel[*model.TaskQueue](temporalMdl)
	stamp.RegisterModel[*model.WorkerDeployment](temporalMdl)
	stamp.RegisterModel[*model.WorkerDeploymentVersion](temporalMdl)
	stamp.RegisterModel[*model.WorkflowClient](temporalMdl)
	stamp.RegisterModel[*model.WorkflowWorker](temporalMdl)
	stamp.RegisterModel[*model.Workflow](temporalMdl)
	stamp.RegisterModel[*model.WorkflowExecution](temporalMdl)
	stamp.RegisterModel[*model.WorkflowExecutionHistory](temporalMdl)
	stamp.RegisterModel[*model.WorkflowTask](temporalMdl)
	stamp.RegisterModel[*model.WorkflowUpdate](temporalMdl)
}

func NewTestSuite(tb testing.TB) *suite {
	s := &suite{Suite: stamp.NewSuite(tb, newLogger)}

	// Once the entire test suite is done, tear down the shared cluster.
	tb.Cleanup(func() {
		s.sharedClusterLock.Lock()
		defer s.sharedClusterLock.Unlock()

		if s.sharedCluster != nil {
			s.sharedCluster.Stop()
		}
	})

	return s
}

func (ts *suite) NewCluster(
	s *stamp.Scenario,
	configs []action.ClusterConfig,
) *Cluster {
	// Create a model environment.
	mdlEnv := stamp.NewModelEnv(s, *temporalMdl, &model.Router{})

	// Create (or re-use) a physical cluster.
	var pc *physicalCluster
	if len(configs) == 0 {
		// When there are configs, we can use a shared cluster.
		ts.sharedClusterLock.Lock()
		defer ts.sharedClusterLock.Unlock()

		if ts.sharedCluster == nil {
			ts.sharedCluster = newPhysicalCluster(s, configs)
		}
		pc = ts.sharedCluster
	} else {
		// When there are custom configs, always start a fresh cluster just for that test.
		pc = newPhysicalCluster(s, configs)
	}

	// Create a cluster actor. Link it to the physical cluster.
	actor := newCluster(s, mdlEnv, configs)
	pc.link(actor)
	s.T().Cleanup(func() { pc.unlink(actor) })

	return actor
}

func (ts *suite) NewWorkflowClient(
	c *Cluster,
	tq *model.TaskQueue,
) *WorkflowClient {
	return newWorkflowClient(c, tq)
}

func (ts *suite) NewWorkflowWorker(
	c *Cluster,
	tq *model.TaskQueue,
) *WorkflowWorker {
	return newWorkflowWorker(c, tq)
}

func (ts *suite) NewWorkflowStack(
	s *stamp.Scenario,
	configs ...action.ClusterConfig,
) (*Cluster, *model.Namespace, *model.TaskQueue, *WorkflowClient, *WorkflowWorker) {
	c := ts.NewCluster(s, configs)
	ns := stamp.Act(c, action.CreateNamespace{})
	tq := stamp.Act(c, action.CreateTaskQueue{Namespace: ns})
	client := ts.NewWorkflowClient(c, tq)
	worker := ts.NewWorkflowWorker(c, tq)
	return c, ns, tq, client, worker
}

func newLogger(t testing.TB) log.Logger {
	tl := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	tl.Expect(testlogger.Error, ".*", tag.FailedAssertion)
	return tl
}
