package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
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

const (
	// Custom scheme for testing custom archiver implementation
	customArchiverScheme = "customtest"
)

type (
	ArchivalSuite struct {
		testcore.FunctionalTestBase

		archivalNamespace   namespace.Name
		archivalNamespaceID namespace.ID

		// Namespace for testing custom archiver
		customArchiverNamespace   namespace.Name
		customArchiverNamespaceID namespace.ID

		// Counters to verify custom archivers are being called
		customHistoryArchiveCalled    atomic.Int32
		customVisibilityArchiveCalled atomic.Int32
	}

	archivalWorkflowInfo struct {
		execution   *commonpb.WorkflowExecution
		branchToken []byte
	}

	// customHistoryArchiver wraps a built-in history archiver and tracks Archive calls
	customHistoryArchiver struct {
		counter *atomic.Int32
	}

	// customVisibilityArchiver wraps a built-in visibility archiver and tracks Archive calls
	customVisibilityArchiver struct {
		counter *atomic.Int32
	}
)

// customHistoryArchiver method implementations
func (c *customHistoryArchiver) Archive(ctx context.Context, uri archiver.URI, request *archiver.ArchiveHistoryRequest, opts ...archiver.ArchiveOption) error {
	c.counter.Add(1)
	return nil
}

func (c *customHistoryArchiver) Get(ctx context.Context, uri archiver.URI, request *archiver.GetHistoryRequest) (*archiver.GetHistoryResponse, error) {
	return nil, nil
}

func (c *customHistoryArchiver) ValidateURI(uri archiver.URI) error {
	return nil
}

// customVisibilityArchiver method implementations
func (c *customVisibilityArchiver) Archive(ctx context.Context, uri archiver.URI, request *archiverspb.VisibilityRecord, opts ...archiver.ArchiveOption) error {
	c.counter.Add(1)
	return nil
}

func (c *customVisibilityArchiver) Query(ctx context.Context, uri archiver.URI, request *archiver.QueryVisibilityRequest, saTypeMap searchattribute.NameTypeMap) (*archiver.QueryVisibilityResponse, error) {
	return nil, nil
}

func (c *customVisibilityArchiver) ValidateURI(uri archiver.URI) error {
	return nil
}

func TestArchivalSuite(t *testing.T) {
	t.Parallel() // This suite can work in parallel as long as it is the only one that use testcore.WithArchivalEnabled() option.
	suite.Run(t, new(ArchivalSuite))
}

func (s *ArchivalSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.ArchivalProcessorArchiveDelay.Key(): time.Duration(0),
	}

	// Create custom history archiver factory for custom scheme
	customHistoryArchiverFactory := provider.CustomHistoryArchiverFactoryFunc(
		func(params provider.NewCustomHistoryArchiverParams) (archiver.HistoryArchiver, error) {
			// Only handle custom scheme, return ErrUnknownScheme for others (including filestore)
			if params.Scheme != customArchiverScheme {
				return nil, provider.ErrUnknownScheme
			}
			// Return a wrapper that delegates to filestore but tracks Archive calls
			return &customHistoryArchiver{
				counter: &s.customHistoryArchiveCalled,
			}, nil
		},
	)

	// Create custom visibility archiver factory for custom scheme
	customVisibilityArchiverFactory := provider.CustomVisibilityArchiverFactoryFunc(
		func(params provider.NewCustomVisibilityArchiverParams) (archiver.VisibilityArchiver, error) {
			// Only handle custom scheme, return ErrUnknownScheme for others (including filestore)
			if params.Scheme != customArchiverScheme {
				return nil, provider.ErrUnknownScheme
			}
			// Return a wrapper that delegates to filestore but tracks Archive calls
			return &customVisibilityArchiver{
				counter: &s.customVisibilityArchiveCalled,
			}, nil
		},
	)

	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(dynamicConfigOverrides),
		testcore.WithArchivalEnabled(),
		testcore.WithCustomHistoryArchiverFactory(customHistoryArchiverFactory),
		testcore.WithCustomVisibilityArchiverFactory(customVisibilityArchiverFactory),
	)

	var err error

	// Register namespace using built-in filestore archiver
	s.archivalNamespace = namespace.Name(testcore.RandomizeStr("archival-enabled-namespace"))
	s.archivalNamespaceID, err = s.RegisterNamespace(
		s.archivalNamespace,
		0, // Archive right away.
		enumspb.ARCHIVAL_STATE_ENABLED,
		s.GetTestCluster().ArchiverBase().HistoryURI(),
		s.GetTestCluster().ArchiverBase().VisibilityURI(),
	)
	s.Require().NoError(err)

	// Register namespace using custom archiver with custom scheme
	s.customArchiverNamespace = namespace.Name(testcore.RandomizeStr("custom-archiver-namespace"))
	customHistoryURI := customArchiverScheme + "://custom-history-archiver"
	customVisibilityURI := customArchiverScheme + "://custom-visibility-archiver"
	s.customArchiverNamespaceID, err = s.RegisterNamespace(
		s.customArchiverNamespace,
		0, // Archive right away.
		enumspb.ARCHIVAL_STATE_ENABLED,
		customHistoryURI,
		customVisibilityURI,
	)
	s.Require().NoError(err)
}

func (s *ArchivalSuite) TearDownSuite() {
	s.Require().NoError(s.MarkNamespaceAsDeleted(s.archivalNamespace))
	s.Require().NoError(s.MarkNamespaceAsDeleted(s.customArchiverNamespace))
	s.FunctionalTestBase.TearDownCluster()
}

func (s *ArchivalSuite) TestArchival_TimerQueueProcessor() {
	s.True(s.GetTestCluster().ArchiverBase().Metadata().GetHistoryConfig().ClusterConfiguredForArchival())

	workflowID := "archival-timer-queue-processor-workflow-id"
	workflowType := "archival-timer-queue-processor-type"
	taskQueue := "archival-timer-queue-processor-task-queue"
	numActivities := 1
	numRuns := 1
	workflowInfo := s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, numActivities, numRuns)[0]

	s.workflowIsArchived(s.archivalNamespaceID, workflowInfo.execution)
	s.historyIsDeleted(workflowInfo)
	s.mutableStateIsDeleted(s.archivalNamespaceID, workflowInfo.execution)
}

func (s *ArchivalSuite) TestArchival_ContinueAsNew() {
	s.True(s.GetTestCluster().ArchiverBase().Metadata().GetHistoryConfig().ClusterConfiguredForArchival())

	workflowID := "archival-continueAsNew-workflow-id"
	workflowType := "archival-continueAsNew-workflow-type"
	taskQueue := "archival-continueAsNew-task-queue"
	numActivities := 1
	numRuns := 5
	workflowInfos := s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, numActivities, numRuns)

	for _, workflowInfo := range workflowInfos {
		s.workflowIsArchived(s.archivalNamespaceID, workflowInfo.execution)
		s.historyIsDeleted(workflowInfo)
		s.mutableStateIsDeleted(s.archivalNamespaceID, workflowInfo.execution)
	}
}

func (s *ArchivalSuite) TestArchival_ArchiverWorker() {
	// s.T().SkipNow() // flaky test, skip for now, will reimplement archival feature.

	s.True(s.GetTestCluster().ArchiverBase().Metadata().GetHistoryConfig().ClusterConfiguredForArchival())

	workflowID := "archival-archiver-worker-workflow-id"
	workflowType := "archival-archiver-worker-workflow-type"
	taskQueue := "archival-archiver-worker-task-queue"
	numActivities := 10
	workflowInfo := s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, numActivities, 1)[0]

	s.workflowIsArchived(s.archivalNamespaceID, workflowInfo.execution)
	s.historyIsDeleted(workflowInfo)
	s.mutableStateIsDeleted(s.archivalNamespaceID, workflowInfo.execution)
}

func (s *ArchivalSuite) TestVisibilityArchival() {
	s.True(s.GetTestCluster().ArchiverBase().Metadata().GetVisibilityConfig().ClusterConfiguredForArchival())

	workflowID := "archival-visibility-workflow-id"
	workflowType := "archival-visibility-workflow-type"
	taskQueue := "archival-visibility-task-queue"
	numActivities := 3
	numRuns := 5
	startTime := time.Now().UnixNano()
	s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, numActivities, numRuns)
	s.startAndFinishWorkflow("some other workflowID", "some other workflow type", taskQueue, s.archivalNamespace, numActivities, numRuns)
	endTime := time.Now().UnixNano()

	var executions []*workflowpb.WorkflowExecutionInfo

	s.EventuallyWithT(func(t *assert.CollectT) {
		request := &workflowservice.ListArchivedWorkflowExecutionsRequest{
			Namespace: s.archivalNamespace.String(),
			PageSize:  2,
			Query:     fmt.Sprintf("CloseTime >= %v and CloseTime <= %v and WorkflowType = '%s'", startTime, endTime, workflowType),
		}
		for len(executions) == 0 || request.NextPageToken != nil {
			response, err := s.FrontendClient().ListArchivedWorkflowExecutions(testcore.NewContext(), request)
			s.NoError(err)
			s.NotNil(response)
			executions = append(executions, response.GetExecutions()...)
			request.NextPageToken = response.NextPageToken
		}
		if len(executions) == numRuns {

			return
		}
		require.Fail(t, "condition was false")

	}, 20*time.Second, 500*time.Millisecond)

	for _, execution := range executions {
		s.Equal(workflowID, execution.GetExecution().GetWorkflowId())
		s.Equal(workflowType, execution.GetType().GetName())
		s.NotZero(execution.StartTime)
		s.NotZero(execution.ExecutionTime)
		s.NotZero(execution.CloseTime)
		s.NotZero(execution.ExecutionDuration)
		s.Equal(
			execution.CloseTime.AsTime().Sub(execution.ExecutionTime.AsTime()),
			execution.ExecutionDuration.AsDuration(),
		)
	}
}

func (s *ArchivalSuite) TestCustomArchiver() {
	s.True(s.GetTestCluster().ArchiverBase().Metadata().GetHistoryConfig().ClusterConfiguredForArchival())

	workflowID := "custom-history-archiver-workflow-id"
	workflowType := "custom-history-archiver-type"
	taskQueue := "custom-history-archiver-task-queue"
	numActivities := 1
	numRuns := 1

	// Reset counter before test
	s.customHistoryArchiveCalled.Store(0)
	s.customVisibilityArchiveCalled.Store(0)

	// Use custom archiver namespace to trigger custom archiver
	s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.customArchiverNamespace, numActivities, numRuns)

	// Verify custom archiver's Archive method was called at least once
	s.EventuallyWithT(func(t *assert.CollectT) {
		called := s.customHistoryArchiveCalled.Load()
		require.Greater(t, called, 0)
	}, 10*time.Second, 500*time.Millisecond, "Custom history archiver Archive method should have been called")
	s.EventuallyWithT(func(t *assert.CollectT) {
		called := s.customVisibilityArchiveCalled.Load()
		require.Greater(t, called, 0)
	}, 10*time.Second, 500*time.Millisecond, "Custom visibility archiver Archive method should have been called")
}

// workflowIsArchived asserts that both the workflow history and workflow visibility are archived.
func (s *ArchivalSuite) workflowIsArchived(namespaceID namespace.ID, execution *commonpb.WorkflowExecution) {
	historyURI, err := archiver.NewURI(s.GetTestCluster().ArchiverBase().HistoryURI())
	s.NoError(err)
	historyArchiver, err := s.GetTestCluster().ArchiverBase().Provider().GetHistoryArchiver(
		historyURI.Scheme(),
	)
	s.NoError(err)

	visibilityURI, err := archiver.NewURI(s.GetTestCluster().ArchiverBase().VisibilityURI())
	s.NoError(err)
	visibilityArchiver, err := s.GetTestCluster().ArchiverBase().Provider().GetVisibilityArchiver(
		visibilityURI.Scheme(),
	)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		ctx := testcore.NewContext()
		var historyResponse *archiver.GetHistoryResponse
		historyResponse, err = historyArchiver.Get(ctx, historyURI, &archiver.GetHistoryRequest{
			NamespaceID: namespaceID.String(),
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			PageSize:    1,
		})
		if err != nil {
			require.Fail(t, "condition was false")

			return
		}
		if len(historyResponse.HistoryBatches) == 0 {
			require.Fail(t, "condition was false")

			return
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
			require.Fail(t, "condition was false")

			return
		}
		if len(visibilityResponse.Executions) > 0 {

			return
		}
		require.Fail(t, "condition was false")

	}, 20*time.Second, 500*time.Millisecond)
}

func (s *ArchivalSuite) historyIsDeleted(workflowInfo archivalWorkflowInfo) {
	shardID := common.WorkflowIDToHistoryShard(
		s.archivalNamespaceID.String(),
		workflowInfo.execution.WorkflowId,
		s.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)

	s.EventuallyWithT(func(t *assert.CollectT) {
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

			return
		}
		s.NoError(err)
		require.Fail(t, "condition was false")

	}, 20*time.Second, 500*time.Millisecond)
}

func (s *ArchivalSuite) mutableStateIsDeleted(namespaceID namespace.ID, execution *commonpb.WorkflowExecution) {
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(),
		s.GetTestClusterConfig().HistoryConfig.NumHistoryShards)
	request := &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}

	s.EventuallyWithT(func(t *assert.CollectT) {
		_, err := s.GetTestCluster().TestBase().ExecutionManager.GetWorkflowExecution(testcore.NewContext(), request)
		if common.IsNotFoundError(err) {

			return
		}
		s.NoError(err)
		require.Fail(t, "condition was false")

	}, 20*time.Second, 500*time.Millisecond)
}

func (s *ArchivalSuite) startAndFinishWorkflow(
	id, wt, tq string,
	nsName namespace.Name,
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
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(startResp.RunId))
	workflowInfos := make([]archivalWorkflowInfo, numRuns)

	workflowComplete := false
	activityCount := int32(numActivities)
	activityCounter := int32(0)
	expectedActivityID := int32(1)
	runCounter := 1

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		branchToken, err := s.getBranchToken(nsName, task.WorkflowExecution)
		s.NoError(err)

		workflowInfos[runCounter-1] = archivalWorkflowInfo{
			execution:   task.WorkflowExecution,
			branchToken: branchToken,
		}

		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.NoError(binary.Write(buf, binary.LittleEndian, activityCounter))
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
		protoassert.ProtoEqual(s.T(), workflowInfos[runCounter-1].execution, task.WorkflowExecution)
		s.Equal(activityName, task.ActivityType.Name)
		currentActivityId, _ := strconv.Atoi(task.ActivityId)
		s.Equal(int(expectedActivityID), currentActivityId)
		s.Equal(expectedActivityID, s.DecodePayloadsByteSliceInt32(task.Input))
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
		Logger:              s.Logger,
		T:                   s.T(),
	}
	for range numRuns {
		for i := range numActivities {
			_, err := poller.PollAndProcessWorkflowTask()
			s.Logger.Info("PollAndProcessWorkflowTask", tag.Error(err))
			s.NoError(err)
			if i%2 == 0 {
				err = poller.PollAndProcessActivityTask(false)
			} else { // just for testing respondActivityTaskCompleteByID
				err = poller.PollAndProcessActivityTaskWithID(false)
			}
			s.Logger.Info("PollAndProcessActivityTask", tag.Error(err))
			s.NoError(err)
		}

		_, err = poller.PollAndProcessWorkflowTask(testcore.WithDumpHistory)
		s.NoError(err)
	}

	s.True(workflowComplete)
	for run := 1; run < numRuns; run++ {
		s.NotEqual(workflowInfos[run-1].execution, workflowInfos[run].execution)
		s.NotEqual(workflowInfos[run-1].branchToken, workflowInfos[run].branchToken)
	}
	return workflowInfos
}

func (s *ArchivalSuite) getBranchToken(
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
