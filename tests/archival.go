// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/protoassert"
)

const (
	retryLimit       = 40
	retryBackoffTime = 500 * time.Millisecond
)

type (
	ArchivalSuite struct {
		*require.Assertions
		FunctionalTestBase
	}

	archivalWorkflowInfo struct {
		execution   *commonpb.WorkflowExecution
		branchToken []byte
	}
)

func (s *ArchivalSuite) SetupSuite() {
	s.setupSuite("testdata/es_cluster.yaml")
}

func (s *ArchivalSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *ArchivalSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func (s *ArchivalSuite) TestArchival_TimerQueueProcessor() {
	s.True(s.testCluster.archiverBase.metadata.GetHistoryConfig().ClusterConfiguredForArchival())

	namespaceID := s.getNamespaceID(s.archivalNamespace)
	workflowID := "archival-timer-queue-processor-workflow-id"
	workflowType := "archival-timer-queue-processor-type"
	taskQueue := "archival-timer-queue-processor-task-queue"
	numActivities := 1
	numRuns := 1
	workflowInfo := s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, namespaceID, numActivities, numRuns)[0]

	s.True(s.isArchived(s.archivalNamespace, workflowInfo.execution))
	s.True(s.isHistoryDeleted(workflowInfo))
	s.True(s.isMutableStateDeleted(namespaceID, workflowInfo.execution))
}

func (s *ArchivalSuite) TestArchival_ContinueAsNew() {
	s.True(s.testCluster.archiverBase.metadata.GetHistoryConfig().ClusterConfiguredForArchival())

	namespaceID := s.getNamespaceID(s.archivalNamespace)
	workflowID := "archival-continueAsNew-workflow-id"
	workflowType := "archival-continueAsNew-workflow-type"
	taskQueue := "archival-continueAsNew-task-queue"
	numActivities := 1
	numRuns := 5
	workflowInfos := s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, namespaceID, numActivities, numRuns)

	for _, workflowInfo := range workflowInfos {
		s.True(s.isArchived(s.archivalNamespace, workflowInfo.execution))
		s.True(s.isHistoryDeleted(workflowInfo))
		s.True(s.isMutableStateDeleted(namespaceID, workflowInfo.execution))
	}
}

func (s *ArchivalSuite) TestArchival_ArchiverWorker() {
	s.T().SkipNow() // flaky test, skip for now, will reimplement archival feature.

	s.True(s.testCluster.archiverBase.metadata.GetHistoryConfig().ClusterConfiguredForArchival())

	namespaceID := s.getNamespaceID(s.archivalNamespace)
	workflowID := "archival-archiver-worker-workflow-id"
	workflowType := "archival-archiver-worker-workflow-type"
	taskQueue := "archival-archiver-worker-task-queue"
	numActivities := 10
	workflowInfo := s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, namespaceID, numActivities, 1)[0]

	s.True(s.isArchived(s.archivalNamespace, workflowInfo.execution))
	s.True(s.isHistoryDeleted(workflowInfo))
	s.True(s.isMutableStateDeleted(namespaceID, workflowInfo.execution))
}

func (s *ArchivalSuite) TestVisibilityArchival() {
	s.True(s.testCluster.archiverBase.metadata.GetVisibilityConfig().ClusterConfiguredForArchival())

	namespaceID := s.getNamespaceID(s.archivalNamespace)
	workflowID := "archival-visibility-workflow-id"
	workflowType := "archival-visibility-workflow-type"
	taskQueue := "archival-visibility-task-queue"
	numActivities := 3
	numRuns := 5
	startTime := time.Now().UnixNano()
	s.startAndFinishWorkflow(workflowID, workflowType, taskQueue, s.archivalNamespace, namespaceID, numActivities, numRuns)
	s.startAndFinishWorkflow("some other workflowID", "some other workflow type", taskQueue, s.archivalNamespace, namespaceID, numActivities, numRuns)
	endTime := time.Now().UnixNano()

	var executions []*workflowpb.WorkflowExecutionInfo

	for i := 0; i != retryLimit; i++ {
		executions = []*workflowpb.WorkflowExecutionInfo{}
		request := &workflowservice.ListArchivedWorkflowExecutionsRequest{
			Namespace: s.archivalNamespace,
			PageSize:  2,
			Query:     fmt.Sprintf("CloseTime >= %v and CloseTime <= %v and WorkflowType = '%s'", startTime, endTime, workflowType),
		}
		for len(executions) == 0 || request.NextPageToken != nil {
			response, err := s.engine.ListArchivedWorkflowExecutions(NewContext(), request)
			s.NoError(err)
			s.NotNil(response)
			executions = append(executions, response.GetExecutions()...)
			request.NextPageToken = response.NextPageToken
		}
		if len(executions) == numRuns {
			break
		}
		time.Sleep(retryBackoffTime)
	}

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

func (s *FunctionalTestBase) getNamespaceID(namespace string) string {
	namespaceResp, err := s.engine.DescribeNamespace(NewContext(), &workflowservice.DescribeNamespaceRequest{
		Namespace: namespace,
	})
	s.NoError(err)
	return namespaceResp.NamespaceInfo.GetId()
}

// isArchived returns true if both the workflow history and workflow visibility are archived.
func (s *ArchivalSuite) isArchived(namespace string, execution *commonpb.WorkflowExecution) bool {
	serviceName := string(primitives.HistoryService)
	historyURI, err := archiver.NewURI(s.testCluster.archiverBase.historyURI)
	s.NoError(err)
	historyArchiver, err := s.testCluster.archiverBase.provider.GetHistoryArchiver(
		historyURI.Scheme(),
		serviceName,
	)
	s.NoError(err)

	visibilityURI, err := archiver.NewURI(s.testCluster.archiverBase.visibilityURI)
	s.NoError(err)
	visibilityArchiver, err := s.testCluster.archiverBase.provider.GetVisibilityArchiver(
		visibilityURI.Scheme(),
		serviceName,
	)
	s.NoError(err)

	for i := 0; i < retryLimit; i++ {
		ctx := NewContext()
		if i > 0 {
			time.Sleep(retryBackoffTime)
		}
		namespaceID := s.getNamespaceID(namespace)
		var historyResponse *archiver.GetHistoryResponse
		historyResponse, err = historyArchiver.Get(ctx, historyURI, &archiver.GetHistoryRequest{
			NamespaceID: namespaceID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			PageSize:    1,
		})
		if err != nil {
			continue
		}
		if len(historyResponse.HistoryBatches) == 0 {
			continue
		}
		var visibilityResponse *archiver.QueryVisibilityResponse
		visibilityResponse, err = visibilityArchiver.Query(
			ctx,
			visibilityURI,
			&archiver.QueryVisibilityRequest{
				NamespaceID: namespaceID,
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
			continue
		}
		if len(visibilityResponse.Executions) > 0 {
			return true
		}
	}
	if err != nil {
		fmt.Println("isArchived failed with error: ", err)
	}
	return false
}

func (s *ArchivalSuite) isHistoryDeleted(
	workflowInfo archivalWorkflowInfo,
) bool {
	namespaceID := s.getNamespaceID(s.archivalNamespace)
	shardID := common.WorkflowIDToHistoryShard(
		namespaceID,
		workflowInfo.execution.WorkflowId,
		s.testClusterConfig.HistoryConfig.NumHistoryShards,
	)

	for i := 0; i < retryLimit; i++ {
		_, err := s.testCluster.testBase.ExecutionManager.ReadHistoryBranch(
			NewContext(),
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

		s.NoError(err)
		time.Sleep(retryBackoffTime)
	}
	return false
}

func (s *ArchivalSuite) isMutableStateDeleted(namespaceID string, execution *commonpb.WorkflowExecution) bool {
	shardID := common.WorkflowIDToHistoryShard(namespaceID, execution.GetWorkflowId(),
		s.testClusterConfig.HistoryConfig.NumHistoryShards)
	request := &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  execution.WorkflowId,
		RunID:       execution.RunId,
	}

	for i := 0; i < retryLimit; i++ {
		_, err := s.testCluster.testBase.ExecutionManager.GetWorkflowExecution(NewContext(), request)
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			return true
		}
		time.Sleep(retryBackoffTime)
	}
	return false
}

func (s *ArchivalSuite) startAndFinishWorkflow(
	id, wt, tq, namespace, namespaceID string,
	numActivities, numRuns int,
) []archivalWorkflowInfo {
	identity := "worker1"
	activityName := "activity_type1"
	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           namespace,
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(1 * time.Second),
		Identity:            identity,
	}
	startResp, err := s.engine.StartWorkflowExecution(NewContext(), request)
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(startResp.RunId))
	workflowInfos := make([]archivalWorkflowInfo, numRuns)

	workflowComplete := false
	activityCount := int32(numActivities)
	activityCounter := int32(0)
	expectedActivityID := int32(1)
	runCounter := 1

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		branchToken, err := s.getBranchToken(namespace, task.WorkflowExecution)
		s.NoError(err)

		workflowInfos[runCounter-1] = archivalWorkflowInfo{
			execution:   task.WorkflowExecution,
			branchToken: branchToken,
		}

		if activityCounter < activityCount {
			activityCounter++
			buf := new(bytes.Buffer)
			s.Nil(binary.Write(buf, binary.LittleEndian, activityCounter))
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
		s.Equal(expectedActivityID, s.decodePayloadsByteSliceInt32(task.Input))
		expectedActivityID++
		return payloads.EncodeString("Activity Result"), false, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           namespace,
		TaskQueue:           taskQueue,
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}
	for run := 0; run < numRuns; run++ {
		for i := 0; i < numActivities; i++ {
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

		_, err = poller.PollAndProcessWorkflowTask(WithDumpHistory)
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
	namespace string,
	execution *commonpb.WorkflowExecution,
) ([]byte, error) {

	descResp, err := s.adminClient.DescribeMutableState(NewContext(), &adminservice.DescribeMutableStateRequest{
		Namespace: namespace,
		Execution: execution,
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
