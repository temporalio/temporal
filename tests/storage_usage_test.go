package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// storageUsageHoldTime is how long the tests hold an execution between two state transitions,
// so that the second transition accrues a non-zero byte-seconds delta.
const storageUsageHoldTime = 250 * time.Millisecond

type StorageUsageSuite struct {
	parallelsuite.Suite[*StorageUsageSuite]
}

func TestStorageUsageSuite(t *testing.T) {
	parallelsuite.Run(t, &StorageUsageSuite{})
}

func (s *StorageUsageSuite) TestStorageUsage_Workflow() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	ctx := s.Context()

	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    env.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	})
	require.NoError(s.T(), err)

	for range 2 {
		time.Sleep(storageUsageHoldTime) //nolint:forbidigo // byte-seconds accrue on wall-clock time between transitions
		_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: startResp.RunId},
			SignalName:        tv.SignalName(),
			Identity:          tv.WorkerIdentity(),
		})
		require.NoError(s.T(), err)
	}

	usage, current := s.describeStorageUsage(ctx, env, tv.WorkflowID(), startResp.RunId, chasm.WorkflowArchetype)
	env.Positive(usage.GetMutableStateByteSeconds())
	env.Positive(usage.GetHistoryByteSeconds())
	env.GreaterOrEqual(current.GetMutableStateByteSeconds(), usage.GetMutableStateByteSeconds())

	// Still running with no new transition: the stored counter stays, the current value keeps growing.
	time.Sleep(storageUsageHoldTime) //nolint:forbidigo // byte-seconds accrue on wall-clock time between transitions
	laterUsage, laterCurrent := s.describeStorageUsage(ctx, env, tv.WorkflowID(), startResp.RunId, chasm.WorkflowArchetype)
	env.True(proto.Equal(usage, laterUsage))
	env.Greater(laterCurrent.GetMutableStateByteSeconds(), current.GetMutableStateByteSeconds())
	env.Greater(laterCurrent.GetHistoryByteSeconds(), current.GetHistoryByteSeconds())
}

func (s *StorageUsageSuite) TestStorageUsage_StandaloneActivity() {
	env := testcore.NewEnv(s.T())
	tv := testvars.New(s.T())
	ctx := s.Context()
	nsValues := []dynamicconfig.ConstrainedValue{
		{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: true},
	}
	cluster := env.GetTestCluster()
	cluster.OverrideDynamicConfig(s.T(), dynamicconfig.EnableChasm, nsValues)
	cluster.OverrideDynamicConfig(s.T(), activity.Enabled, nsValues)

	startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          tv.ActivityID(),
		ActivityType:        tv.ActivityType(),
		Identity:            tv.WorkerIdentity(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tv.TaskQueue().GetName()},
		StartToCloseTimeout: durationpb.New(time.Minute),
		RequestId:           uuid.NewString(),
	})
	require.NoError(s.T(), err)

	time.Sleep(storageUsageHoldTime) //nolint:forbidigo // byte-seconds accrue on wall-clock time between transitions
	_, err = env.FrontendClient().TerminateActivityExecution(ctx, &workflowservice.TerminateActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: tv.ActivityID(),
		RunId:      startResp.GetRunId(),
		Reason:     "storage usage test",
	})
	require.NoError(s.T(), err)

	usage, current := s.describeStorageUsage(ctx, env, tv.ActivityID(), startResp.GetRunId(), activity.Archetype)
	env.Positive(usage.GetMutableStateByteSeconds())
	// Closed: accrual has stopped, so the current value is the stored one.
	env.True(proto.Equal(usage, current))
}

func (s *StorageUsageSuite) describeStorageUsage(
	ctx context.Context,
	env *testcore.TestEnv,
	businessID string,
	runID string,
	archetype string,
) (stored *persistencespb.StorageUsage, current *persistencespb.StorageUsage) {
	resp, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: businessID, RunId: runID},
		Archetype: archetype,
	})
	require.NoError(s.T(), err)
	return resp.GetDatabaseMutableState().GetExecutionInfo().GetExecutionStats().GetStorageUsage(), resp.GetCurrentStorageUsage()
}
