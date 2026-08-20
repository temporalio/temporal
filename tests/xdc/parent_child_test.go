package xdc

import (
	"testing"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tests/testcore"
)

func TestParentChildXDCTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(parentChildXDCTestSuite))
}

func (s *parentChildXDCTestSuite) SetupSuite() {
	s.enableTransitionHistory = true
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():       true,
		dynamicconfig.EnableReplicationTaskBatching.Key(): false,
	}
	s.logger = log.NewTestLogger()
	s.setupSuite(testcore.WithNumHistoryShards(2))
}

func (s *parentChildXDCTestSuite) SetupTest() {
	s.setupTest()
}

func (s *parentChildXDCTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestReproOrphanedChildAfterForceFailover passes when the known bug is reproduced: the new active
// parent abandons a running child from its losing branch as WORKFLOW_ALREADY_EXISTS.
func (s *parentChildXDCTestSuite) TestReproOrphanedChildAfterForceFailover() {
	s.runParentChildScenario(parentChildScenario{
		steps: []parentChildScenarioStep{
			startParentWorkflow(),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			),
			completeParentWorkflowTaskWithStartChildCommand(),
			holdReplicationAtTaskContainingEvent(
				initialStandbyCluster,
				parentWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			),
			applyReplicationThroughTaskContainingEvent(
				initialStandbyCluster,
				childWorkflow,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			),
			failoverNamespaceTo(initialStandbyCluster),
			refreshParentWorkflowTasks(),
			completeParentWorkflowTaskWithStartChildCommand(),
		},
		expectations: []parentChildExpectation{
			parentStartChildFailed(enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS),
			childIsOrphaned(),
			childHasStatus(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
		},
	})
}
