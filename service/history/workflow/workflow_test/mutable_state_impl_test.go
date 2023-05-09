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

// Package workflow_test contains tests for the workflow package. There are also tests in the workflow package itself,
// but test packages force you to only test exported methods.
// See https://github.com/maratori/testpackage#motivation for more on the rationale used here.
package workflow_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

func TestMutableStateImpl_ForceFlushBufferedEvents(t *testing.T) {
	t.Parallel()

	for _, tc := range []mutationTestCase{
		{
			name:              "Number of events ok",
			transactionPolicy: workflow.TransactionPolicyActive,
			signals:           2,
			maxEvents:         2,
			maxSizeInBytes:    math.MaxInt,
			expectFlush:       false,
		},
		{
			name:              "Max number of events exceeded",
			transactionPolicy: workflow.TransactionPolicyActive,
			signals:           3,
			maxEvents:         2,
			maxSizeInBytes:    math.MaxInt,
			expectFlush:       true,
		},
		{
			name:              "Number of events ok but byte size limit exceeded",
			transactionPolicy: workflow.TransactionPolicyActive,
			signals:           2,
			maxEvents:         2,
			maxSizeInBytes:    25,
			expectFlush:       true,
		},
		{
			name:              "Max number of events and size of events both exceeded",
			transactionPolicy: workflow.TransactionPolicyActive,
			signals:           3,
			maxEvents:         2,
			maxSizeInBytes:    25,
			expectFlush:       true,
		},
	} {
		tc := tc
		t.Run(tc.name, tc.Run)
	}
}

type mutationTestCase struct {
	name              string
	transactionPolicy workflow.TransactionPolicy
	signals           int
	maxEvents         int
	expectFlush       bool
	maxSizeInBytes    int
}

func (c *mutationTestCase) Run(t *testing.T) {
	t.Parallel()

	nsEntry := tests.LocalNamespaceEntry
	ms := c.createMutableState(t, nsEntry)

	c.startWorkflowExecution(t, ms, nsEntry)

	wft := c.startWFT(t, ms)

	for i := 0; i < c.signals; i++ {
		c.addWorkflowExecutionSignaled(t, i, ms)
	}

	_, workflowEvents, err := ms.CloseTransactionAsMutation(c.transactionPolicy)
	if err != nil {
		t.Fatal(err)
	}

	if c.expectFlush {
		c.testFailure(t, ms, wft, workflowEvents)
	} else {
		c.testSuccess(t, ms, workflowEvents)
	}
}

func (c *mutationTestCase) startWFT(
	t *testing.T,
	ms *workflow.MutableStateImpl,
) *workflow.WorkflowTaskInfo {
	t.Helper()

	wft, err := ms.AddWorkflowTaskScheduledEvent(false, enums.WORKFLOW_TASK_TYPE_NORMAL)
	if err != nil {
		t.Fatal(err)
	}

	_, wft, err = ms.AddWorkflowTaskStartedEvent(wft.ScheduledEventID, wft.RequestID, wft.TaskQueue, "")
	if err != nil {
		t.Fatal(err)
	}

	return wft
}

func (c *mutationTestCase) startWorkflowExecution(
	t *testing.T,
	ms *workflow.MutableStateImpl,
	nsEntry *namespace.Namespace,
) {
	t.Helper()

	_, err := ms.AddWorkflowExecutionStartedEvent(
		commonpb.WorkflowExecution{
			WorkflowId: "694B31C3-1FDC-4C9E-87BC-747D539BF0CD",
			RunId:      "3E1836B8-8692-440D-9495-45D9EABDED6B",
		},
		&historyservice.StartWorkflowExecutionRequest{
			Attempt:     1,
			NamespaceId: nsEntry.ID().String(),
			StartRequest: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType:        &commonpb.WorkflowType{Name: "workflow-type"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: "task-queue-name"},
				WorkflowRunTimeout:  timestamp.DurationPtr(200 * time.Second),
				WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func (c *mutationTestCase) addWorkflowExecutionSignaled(t *testing.T, i int, ms *workflow.MutableStateImpl) {
	t.Helper()

	payload := &commonpb.Payloads{}
	identity := fmt.Sprintf("%d", i)
	header := &commonpb.Header{}

	_, err := ms.AddWorkflowExecutionSignaled(
		"signal-name",
		payload,
		identity,
		header,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func (c *mutationTestCase) createMutableState(t *testing.T, nsEntry *namespace.Namespace) *workflow.MutableStateImpl {
	t.Helper()

	ctrl := gomock.NewController(t)
	cfg := c.createConfig()
	shardContext := shard.NewTestContext(ctrl, &persistencespb.ShardInfo{}, cfg)

	nsRegistry := shardContext.Resource.NamespaceCache
	nsRegistry.EXPECT().GetNamespaceByID(nsEntry.ID()).Return(nsEntry, nil).AnyTimes()

	clusterMetadata := shardContext.Resource.ClusterMetadata
	clusterMetadata.EXPECT().ClusterNameForFailoverVersion(nsEntry.IsGlobalNamespace(),
		nsEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	executionManager := shardContext.Resource.ExecutionMgr
	executionManager.EXPECT().GetHistoryBranchUtil().Return(&persistence.HistoryBranchUtilImpl{}).AnyTimes()

	startTime := time.Time{}
	logger := log.NewNoopLogger()
	eventsCache := events.NewMockCache(ctrl)
	eventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	ms := workflow.NewMutableState(
		shardContext,
		eventsCache,
		logger,
		nsEntry,
		startTime,
	)
	ms.GetExecutionInfo().NamespaceId = nsEntry.ID().String()
	// must start with a non-empty version history so that we have something to compare against when writing
	ms.GetExecutionInfo().VersionHistories.Histories[0].Items = []*historyspb.VersionHistoryItem{
		{Version: 0, EventId: 1},
	}

	return ms
}

func (c *mutationTestCase) createConfig() *configs.Config {
	cfg := tests.NewDynamicConfig()
	cfg.MaximumBufferedEventsBatch = c.getMaxEvents
	cfg.MaximumBufferedEventsSizeInBytes = c.getMaxSizeInBytes

	return cfg
}

func (c *mutationTestCase) getMaxEvents() int {
	return c.maxEvents
}

func (c *mutationTestCase) getMaxSizeInBytes() int {
	return c.maxSizeInBytes
}

func (c *mutationTestCase) testWFTFailedEvent(
	t *testing.T,
	wft *workflow.WorkflowTaskInfo,
	event *history.HistoryEvent,
) {
	t.Helper()

	attr := event.GetWorkflowTaskFailedEventAttributes()
	if attr == nil {
		t.Fatal("WFT-failed event has nil attributes")
	}

	if attr.ScheduledEventId != wft.ScheduledEventID || attr.StartedEventId != wft.StartedEventID {
		t.Errorf("WFT-failed event, %#v, does not target our WFT, %#v", event, wft)
	}

	if attr.Cause != enumspb.WORKFLOW_TASK_FAILED_CAUSE_FORCE_CLOSE_COMMAND {
		t.Errorf(
			"WFT should fail because it was force closed, but the failure cause is %q instead",
			attr.Cause.String(),
		)
	}
}

func (c *mutationTestCase) findWFTEvent(eventType enumspb.EventType, workflowEvents []*persistence.WorkflowEvents) (
	*history.HistoryEvent,
	bool,
) {
	for _, batch := range workflowEvents {
		for _, ev := range batch.Events {
			if ev.EventType == eventType {
				return ev, true
			}
		}
	}

	return nil, false
}

func (c *mutationTestCase) testFailure(
	t *testing.T,
	ms *workflow.MutableStateImpl,
	wft *workflow.WorkflowTaskInfo,
	workflowEvents []*persistence.WorkflowEvents,
) {
	t.Helper()

	wftAttempt := ms.GetExecutionInfo().GetWorkflowTaskAttempt()
	if wftAttempt != 2 {
		t.Errorf("Expected WFT attempt number to be 2 if the WFT failed, but was %d", wftAttempt)
	}

	event, ok := c.findWFTEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, workflowEvents)
	if !ok {
		t.Fatal("Failed to find WFT-failed event in history")
	}

	flushedSignals := 0

	for _, batch := range workflowEvents {
		for _, ev := range batch.Events {
			if ev.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
				flushedSignals++
			}
		}
	}

	if flushedSignals != c.signals {
		t.Errorf(
			"Expected number of flushed signals, %d, to equal the number of signals in this WFT, %d",
			flushedSignals,
			c.signals,
		)
	}

	c.testWFTFailedEvent(t, wft, event)
}

func (c *mutationTestCase) testSuccess(
	t *testing.T,
	ms *workflow.MutableStateImpl,
	workflowEvents []*persistence.WorkflowEvents,
) {
	t.Helper()

	_, ok := c.findWFTEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, workflowEvents)
	if ok {
		t.Fatalf("Expected to not find a WFT-failed event in %#v", workflowEvents)
	}

	wftAttempt := ms.GetExecutionInfo().GetWorkflowTaskAttempt()
	if wftAttempt != 1 {
		t.Errorf("Expected WFT attempt number to be unchanged if the WFT succeeded, but is now %d", wftAttempt)
	}
}
