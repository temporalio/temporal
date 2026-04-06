package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TimeSkippingTestSuite struct {
	parallelsuite.Suite[*TimeSkippingTestSuite]
}

func TestTimeSkippingTestSuite(t *testing.T) {
	parallelsuite.Run(t, &TimeSkippingTestSuite{})
}

// TestTimeSkipping_FeatureDisabled verifies that starting a workflow with time skipping
// returns an error when the feature flag is off for the namespace.
func (s *TimeSkippingTestSuite) TestTimeSkipping_FeatureDisabled() {
	env := testcore.NewEnv(s.T())
	// TimeSkippingEnabled defaults to false; no override needed.
	id := "functional-timeskipping-feature-disabled"
	tl := "functional-timeskipping-feature-disabled-tq"

	_, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.Error(err, "expected error when time skipping is disabled for namespace")
}

// TestTimeSkipping_StartWorkflow_DCEnabled verifies that StartWorkflowExecution with
// TimeSkippingConfig persists the config in mutable state when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_StartWorkflow_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	inputBound := &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
		MaxSkippedDuration: durationpb.New(10 * time.Second),
	}

	resp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true, DisablePropagation: false, Bound: inputBound},
	})
	s.NoError(err)

	ms := s.getMutableState(env, tv.WorkflowID(), resp.RunId)
	s.True(ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig().GetEnabled())
	s.True(proto.Equal(&workflowpb.TimeSkippingConfig{
		Enabled:            true,
		DisablePropagation: false,
		Bound:              inputBound,
	}, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

// TestTimeSkipping_SignalWithStart_DCEnabled verifies that SignalWithStartWorkflowExecution
// with TimeSkippingConfig persists the config in mutable state when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_SignalWithStart_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	inputBound := &workflowpb.TimeSkippingConfig_MaxElapsedDuration{
		MaxElapsedDuration: durationpb.New(10 * time.Second),
	}

	resp, err := env.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		SignalName:          tv.SignalName(),
		TimeSkippingConfig: &workflowpb.TimeSkippingConfig{
			Enabled:            true,
			DisablePropagation: true,
			Bound:              inputBound,
		},
	})
	s.NoError(err)

	ms := s.getMutableState(env, tv.WorkflowID(), resp.RunId)
	s.True(proto.Equal(&workflowpb.TimeSkippingConfig{
		Enabled:            true,
		DisablePropagation: true,
		Bound:              inputBound,
	}, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

// TestTimeSkipping_ExecuteMultiOperation_DCEnabled verifies that a StartWorkflow inside
// ExecuteMultiOperation with TimeSkippingConfig persists the config in mutable state
// when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_ExecuteMultiOperation_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	inputConfig := &workflowpb.TimeSkippingConfig{
		Enabled:            true,
		DisablePropagation: true,
		Bound: &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
			MaxSkippedDuration: durationpb.New(30 * time.Second),
		},
	}

	resp, err := env.FrontendClient().ExecuteMultiOperation(testcore.NewContext(), &workflowservice.ExecuteMultiOperationRequest{
		Namespace: env.Namespace().String(),
		Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
			{
				Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
					StartWorkflow: &workflowservice.StartWorkflowExecutionRequest{
						RequestId:           uuid.NewString(),
						Namespace:           env.Namespace().String(),
						WorkflowId:          tv.WorkflowID(),
						WorkflowType:        tv.WorkflowType(),
						TaskQueue:           tv.TaskQueue(),
						WorkflowRunTimeout:  durationpb.New(100 * time.Second),
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
						TimeSkippingConfig:  inputConfig,
					},
				},
			},
			{
				Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
					UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionRequest{
						Namespace:         env.Namespace().String(),
						WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
						Request: &updatepb.Request{
							Meta:  &updatepb.Meta{UpdateId: uuid.NewString()},
							Input: &updatepb.Input{Name: "my-update"},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	runID := resp.GetResponses()[0].GetStartWorkflow().GetRunId()
	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(inputConfig, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

func (s *TimeSkippingTestSuite) getMutableState(env *testcore.TestEnv, workflowID, runID string) *persistence.GetWorkflowExecutionResponse {
	shardID := common.WorkflowIDToHistoryShard(
		env.NamespaceID().String(),
		workflowID,
		env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)
	ms, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(testcore.NewContext(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  workflowID,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)
	return ms
}
