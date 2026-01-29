package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type archivalWorkflowInfo struct {
	execution   *commonpb.WorkflowExecution
	branchToken []byte
}

// archivalTestEnv defines the interface for test environment used in archival tests.
// This is needed because testcore.testEnv is unexported but we need its methods in helpers.
type archivalTestEnv interface {
	testcore.Env
	RegisterNamespace(nsName namespace.Name, retentionDays int32, archivalState enumspb.ArchivalState, historyURI string, visibilityURI string) (namespace.ID, error)
	AdminClient() adminservice.AdminServiceClient
	GetTestClusterConfig() *testcore.TestClusterConfig
}

func TestArchival(t *testing.T) {
	t.Run("TimerQueueProcessor", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithClusterOptions(testcore.WithArchivalEnabled()),
			testcore.WithDynamicConfig(dynamicconfig.ArchivalProcessorArchiveDelay, time.Duration(0)),
		)

		archivalNamespace, archivalNamespaceID := registerArchivalNamespace(t, s)

		require.True(t, s.GetTestCluster().ArchiverBase().Metadata().GetHistoryConfig().ClusterConfiguredForArchival())

		workflowID := "archival-timer-queue-processor-workflow-id"
		workflowType := "archival-timer-queue-processor-type"
		taskQueue := "archival-timer-queue-processor-task-queue"
		numActivities := 1
		numRuns := 1
		workflowInfo := startAndFinishWorkflowForArchival(t, s, s.Logger, workflowID, workflowType, taskQueue, archivalNamespace, archivalNamespaceID, numActivities, numRuns)[0]

		assertWorkflowIsArchived(t, s, archivalNamespaceID, workflowInfo.execution)
		assertHistoryIsDeleted(t, s, archivalNamespaceID, workflowInfo)
		assertMutableStateIsDeleted(t, s, archivalNamespaceID, workflowInfo.execution)
	})

	t.Run("ContinueAsNew", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithClusterOptions(testcore.WithArchivalEnabled()),
			testcore.WithDynamicConfig(dynamicconfig.ArchivalProcessorArchiveDelay, time.Duration(0)),
		)

		archivalNamespace, archivalNamespaceID := registerArchivalNamespace(t, s)

		require.True(t, s.GetTestCluster().ArchiverBase().Metadata().GetHistoryConfig().ClusterConfiguredForArchival())

		workflowID := "archival-continueAsNew-workflow-id"
		workflowType := "archival-continueAsNew-workflow-type"
		taskQueue := "archival-continueAsNew-task-queue"
		numActivities := 1
		numRuns := 5
		workflowInfos := startAndFinishWorkflowForArchival(t, s, s.Logger, workflowID, workflowType, taskQueue, archivalNamespace, archivalNamespaceID, numActivities, numRuns)

		for _, workflowInfo := range workflowInfos {
			assertWorkflowIsArchived(t, s, archivalNamespaceID, workflowInfo.execution)
			assertHistoryIsDeleted(t, s, archivalNamespaceID, workflowInfo)
			assertMutableStateIsDeleted(t, s, archivalNamespaceID, workflowInfo.execution)
		}
	})

	t.Run("ArchiverWorker", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithClusterOptions(testcore.WithArchivalEnabled()),
			testcore.WithDynamicConfig(dynamicconfig.ArchivalProcessorArchiveDelay, time.Duration(0)),
		)

		archivalNamespace, archivalNamespaceID := registerArchivalNamespace(t, s)

		require.True(t, s.GetTestCluster().ArchiverBase().Metadata().GetHistoryConfig().ClusterConfiguredForArchival())

		workflowID := "archival-archiver-worker-workflow-id"
		workflowType := "archival-archiver-worker-workflow-type"
		taskQueue := "archival-archiver-worker-task-queue"
		numActivities := 10
		workflowInfo := startAndFinishWorkflowForArchival(t, s, s.Logger, workflowID, workflowType, taskQueue, archivalNamespace, archivalNamespaceID, numActivities, 1)[0]

		assertWorkflowIsArchived(t, s, archivalNamespaceID, workflowInfo.execution)
		assertHistoryIsDeleted(t, s, archivalNamespaceID, workflowInfo)
		assertMutableStateIsDeleted(t, s, archivalNamespaceID, workflowInfo.execution)
	})

	t.Run("VisibilityArchival", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithClusterOptions(testcore.WithArchivalEnabled()),
			testcore.WithDynamicConfig(dynamicconfig.ArchivalProcessorArchiveDelay, time.Duration(0)),
		)

		archivalNamespace, _ := registerArchivalNamespace(t, s)

		require.True(t, s.GetTestCluster().ArchiverBase().Metadata().GetVisibilityConfig().ClusterConfiguredForArchival())

		workflowID := "archival-visibility-workflow-id"
		workflowType := "archival-visibility-workflow-type"
		taskQueue := "archival-visibility-task-queue"
		numActivities := 3
		numRuns := 5
		startTime := time.Now().UnixNano()
		startAndFinishWorkflowForArchival(t, s, s.Logger, workflowID, workflowType, taskQueue, archivalNamespace, "", numActivities, numRuns)
		startAndFinishWorkflowForArchival(t, s, s.Logger, "some other workflowID", "some other workflow type", taskQueue, archivalNamespace, "", numActivities, numRuns)
		endTime := time.Now().UnixNano()

		var executions []*workflowpb.WorkflowExecutionInfo

		require.Eventually(t, func() bool {
			request := &workflowservice.ListArchivedWorkflowExecutionsRequest{
				Namespace: archivalNamespace.String(),
				PageSize:  2,
				Query:     fmt.Sprintf("CloseTime >= %v and CloseTime <= %v and WorkflowType = '%s'", startTime, endTime, workflowType),
			}
			for len(executions) == 0 || request.NextPageToken != nil {
				response, err := s.FrontendClient().ListArchivedWorkflowExecutions(testcore.NewContext(), request)
				require.NoError(t, err)
				require.NotNil(t, response)
				executions = append(executions, response.GetExecutions()...)
				request.NextPageToken = response.NextPageToken
			}
			if len(executions) == numRuns {
				return true
			}
			return false
		}, 20*time.Second, 500*time.Millisecond)

		for _, execution := range executions {
			require.Equal(t, workflowID, execution.GetExecution().GetWorkflowId())
			require.Equal(t, workflowType, execution.GetType().GetName())
			require.NotZero(t, execution.StartTime)
			require.NotZero(t, execution.ExecutionTime)
			require.NotZero(t, execution.CloseTime)
			require.NotZero(t, execution.ExecutionDuration)
			require.Equal(t,
				execution.CloseTime.AsTime().Sub(execution.ExecutionTime.AsTime()),
				execution.ExecutionDuration.AsDuration(),
			)
		}
	})
}

func registerArchivalNamespace(t *testing.T, s archivalTestEnv) (namespace.Name, namespace.ID) {
	archivalNamespace := namespace.Name(testcore.RandomizeStr("archival-enabled-namespace"))
	archivalNamespaceID, err := s.RegisterNamespace(
		archivalNamespace,
		0, // Archive right away.
		enumspb.ARCHIVAL_STATE_ENABLED,
		s.GetTestCluster().ArchiverBase().HistoryURI(),
		s.GetTestCluster().ArchiverBase().VisibilityURI(),
	)
	require.NoError(t, err)
	return archivalNamespace, archivalNamespaceID
}

// assertWorkflowIsArchived asserts that both the workflow history and workflow visibility are archived.
func assertWorkflowIsArchived(t *testing.T, s archivalTestEnv, namespaceID namespace.ID, execution *commonpb.WorkflowExecution) {
	historyURI, err := archiver.NewURI(s.GetTestCluster().ArchiverBase().HistoryURI())
	require.NoError(t, err)
	historyArchiver, err := s.GetTestCluster().ArchiverBase().Provider().GetHistoryArchiver(
		historyURI.Scheme(),
	)
	require.NoError(t, err)

	visibilityURI, err := archiver.NewURI(s.GetTestCluster().ArchiverBase().VisibilityURI())
	require.NoError(t, err)
	visibilityArchiver, err := s.GetTestCluster().ArchiverBase().Provider().GetVisibilityArchiver(
		visibilityURI.Scheme(),
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ctx := testcore.NewContext()
		var historyResponse *archiver.GetHistoryResponse
		historyResponse, err = historyArchiver.Get(ctx, historyURI, &archiver.GetHistoryRequest{
			NamespaceID: namespaceID.String(),
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			PageSize:    1,
		})
		if err != nil {
			return false
		}
		if len(historyResponse.HistoryBatches) == 0 {
			return false
		}
		var visibilityResponse *archiver.QueryVisibilityResponse
		visibilityResponse, err = visibilityArchiver.Query(
			ctx,
			visibilityURI,
			&archiver.QueryVisibilityRequest{
				NamespaceID: namespaceID.String(),
				PageSize:    1,
				Query: fmt.Sprintf(
					"WorkflowId = '%s' and RunId = '%s'",
					execution.GetWorkflowId(),
					execution.GetRunId(),
				),
			},
			searchattribute.NameTypeMap{},
		)
		if err != nil {
			return false
		}
		if len(visibilityResponse.Executions) > 0 {
			return true
		}
		return false
	}, 20*time.Second, 500*time.Millisecond)
}

func assertHistoryIsDeleted(t *testing.T, s archivalTestEnv, namespaceID namespace.ID, workflowInfo archivalWorkflowInfo) {
	shardID := common.WorkflowIDToHistoryShard(
		namespaceID.String(),
		workflowInfo.execution.WorkflowId,
		s.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)

	require.Eventually(t, func() bool {
		_, err := s.GetTestCluster().TestBase().ExecutionManager.ReadHistoryBranch(
			testcore.NewContext(),
			&persistence.ReadHistoryBranchRequest{
				ShardID:       shardID,
				BranchToken:   workflowInfo.branchToken,
				MinEventID:    common.FirstEventID,
				MaxEventID:    common.EndEventID,
				PageSize:      1,
				NextPageToken: nil,
			},
		)
		if common.IsNotFoundError(err) {
			return true
		}
		require.NoError(t, err)
		return false
	}, 20*time.Second, 500*time.Millisecond)
}

func assertMutableStateIsDeleted(t *testing.T, s archivalTestEnv, namespaceID namespace.ID, execution *commonpb.WorkflowExecution) {
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(),
		s.GetTestClusterConfig().HistoryConfig.NumHistoryShards)
	request := &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}

	require.Eventually(t, func() bool {
		_, err := s.GetTestCluster().TestBase().ExecutionManager.GetWorkflowExecution(testcore.NewContext(), request)
		if common.IsNotFoundError(err) {
			return true
		}
		require.NoError(t, err)
		return false
	}, 20*time.Second, 500*time.Millisecond)
}

func startAndFinishWorkflowForArchival(
	t *testing.T,
	s archivalTestEnv,
	logger log.Logger,
	id, wt, tq string,
	nsName namespace.Name,
	nsID namespace.ID,
	numActivities, numRuns int,
) []archivalWorkflowInfo {
	identity := "worker1"
	activityName := "activity_type1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           nsName.String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), request)
	require.NoError(t, err)
	logger.Info("StartWorkflowExecution", tag.WorkflowRunID(startResp.RunId))
	workflowInfos := make([]archivalWorkflowInfo, numRuns)

	workflowComplete := false
	activityCount := int32(numActivities)
	activityCounter := int32(0)
	expectedActivityID := int32(1)
	runCounter := 1

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		branchToken, err := getBranchTokenForArchival(s, nsName, task.WorkflowExecution)
		require.NoError(t, err)

		workflowInfos[runCounter-1] = archivalWorkflowInfo{
			execution:   task.WorkflowExecution,
			branchToken: branchToken,
		}

		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			require.Nil(t, binary.Write(buf, binary.LittleEndian, activityCounter))
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:             convert.Int32ToString(activityCounter),
					ActivityType:           &commonpb.ActivityType{Name: activityName},
					TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:                  payloads.EncodeBytes(buf.Bytes()),
					ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
					ScheduleToStartTimeout: durationpb.New(10 * time.Second),
					StartToCloseTimeout:    durationpb.New(50 * time.Second),
					HeartbeatTimeout:       durationpb.New(5 * time.Second),
				}},
			}}, nil
		}

		if runCounter < numRuns {
			activityCounter = int32(0)
			expectedActivityID = int32(1)
			runCounter++
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
					WorkflowType:        workflowType,
					TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:               nil,
					WorkflowRunTimeout:  durationpb.New(100 * time.Second),
					WorkflowTaskTimeout: durationpb.New(1 * time.Second),
				}},
			}}, nil
		}

		workflowComplete = true
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads.EncodeString("Done"),
			}},
		}}, nil
	}

	atHandler := func(task *workflowservice.PollActivityTaskQueueResponse) (*commonpb.Payloads, bool, error) {
		protoassert.ProtoEqual(t, workflowInfos[runCounter-1].execution, task.WorkflowExecution)
		require.Equal(t, activityName, task.ActivityType.Name)
		currentActivityId, _ := strconv.Atoi(task.ActivityId)
		require.Equal(t, int(expectedActivityID), currentActivityId)
		// Decode payload to int32
		var buf []byte
		require.NoError(t, payloads.Decode(task.Input, &buf))
		var actualActivityID int32
		require.NoError(t, binary.Read(bytes.NewReader(buf), binary.LittleEndian, &actualActivityID))
		require.Equal(t, expectedActivityID, actualActivityID)
		expectedActivityID++
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           nsName.String(),
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              logger,
		T:                   t,
	}
	for run := 0; run < numRuns; run++ {
		for i := 0; i < numActivities; i++ {
			_, err := poller.PollAndProcessWorkflowTask()
			logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
			require.NoError(t, err)
			if i%2 == 0 {
				err = poller.PollAndProcessActivityTask(false)
			} else { // just for testing respondActivityTaskCompleteByID
				err = poller.PollAndProcessActivityTaskWithID(false)
			}
			logger.Info("PollAndProcessActivityTask", tag.Error(err))
			require.NoError(t, err)
		}

		_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
		require.NoError(t, err)
	}

	require.True(t, workflowComplete)
	for run := 1; run < numRuns; run++ {
		require.NotEqual(t, workflowInfos[run-1].execution, workflowInfos[run].execution)
		require.NotEqual(t, workflowInfos[run-1].branchToken, workflowInfos[run].branchToken)
	}
	return workflowInfos
}

func getBranchTokenForArchival(
	s archivalTestEnv,
	nsName namespace.Name,
	execution *commonpb.WorkflowExecution,
) ([]byte, error) {

	descResp, err := s.AdminClient().DescribeMutableState(testcore.NewContext(), &adminservice.DescribeMutableStateRequest{
		Namespace: nsName.String(),
		Execution: execution,
		Archetype: chasm.WorkflowArchetype,
	})
	if err != nil {
		return nil, err
	}

	versionHistories := descResp.CacheMutableState.ExecutionInfo.VersionHistories
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return nil, err
	}

	return currentVersionHistory.GetBranchToken(), nil
}
