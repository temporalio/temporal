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

package queues_test

import (
	"errors"
	"testing"
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type (
	// testLogger records info and error logs.
	testLogger struct {
		log.Logger
		tags      []tag.Tag
		infoLogs  []logRecord
		errorLogs []logRecord
	}
	// logRecord represents a call to a method of [log.Logger]
	logRecord struct {
		msg  string
		tags []tag.Tag
	}
	// laggyExecutableDLQ is a fake [queues.Executable] that takes a configurable amount of time to execute.
	laggyExecutableDLQ struct {
		queues.Executable
		timeToExecute time.Duration
		timeSource    *clock.EventTimeSource
	}
)

var errTerminal = new(serialization.DeserializationError)

func (t *testLogger) Info(msg string, tags ...tag.Tag) {
	t.infoLogs = append(t.infoLogs, logRecord{
		msg:  msg,
		tags: append(t.tags, tags...),
	})
}

func (t *testLogger) Error(msg string, tags ...tag.Tag) {
	t.errorLogs = append(t.errorLogs, logRecord{
		msg:  msg,
		tags: append(t.tags, tags...),
	})
}

func (t *testLogger) WithTags(tags ...tag.Tag) log.Logger {
	tmp := *t
	tmp.tags = append(tmp.tags, tags...)
	return &tmp
}

func (l *laggyExecutableDLQ) Execute() error {
	l.timeSource.Advance(l.timeToExecute)
	return l.Executable.Execute()
}

func TestExecutableDLQObserver(t *testing.T) {
	// Test that the metrics and logging behave correctly for the ExecutableDLQObserver.

	t.Parallel()

	ctrl := gomock.NewController(t)
	namespaceEntry := tests.GlobalNamespaceEntry
	task := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			string(namespaceEntry.ID()),
			"test-workflow-id",
			"test-run-id",
		),
		TaskID: 42,
	}
	timeSource := clock.NewEventTimeSource()

	dlq := &queuestest.FakeDLQ{}
	logger := &testLogger{}
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	executable := queuestest.NewFakeExecutable(task, errTerminal)
	executableDLQ := &laggyExecutableDLQ{
		Executable:    newExecutableDLQ(executable, dlq),
		timeToExecute: time.Second,
		timeSource:    timeSource,
	}
	numHistoryShards := 5
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(namespaceEntry, nil).AnyTimes()
	dlqObserver := queues.NewExecutableDLQObserver(executableDLQ, numHistoryShards, namespaceRegistry, timeSource, logger, metricsHandler)
	dlq.EnqueueTaskErr = errors.New("some error writing to DLQ")

	// 1. First call to execute should indicate that the task failed, logging the task and cause of the failure.
	err := dlqObserver.Execute()
	assert.ErrorIs(t, err, queues.ErrTerminalTaskFailure)
	snapshot := capture.Snapshot()
	verifySample(t, snapshot[metrics.TaskTerminalFailures.GetMetricName()], int64(1))

	shardID := tasks.GetShardIDForTask(task, numHistoryShards)
	expectedTags := []tag.Tag{
		tag.WorkflowNamespaceID(string(namespaceEntry.ID())),
		tag.WorkflowID("test-workflow-id"),
		tag.WorkflowRunID("test-run-id"),
		tag.ShardID(int32(shardID)),
		tag.TaskID(42),
	}
	expectedErrorLogs := []logRecord{
		{
			msg:  "A terminal error occurred while processing this task",
			tags: append(expectedTags, tag.Error(err)),
		},
	}
	verifyLogsEqual(t, expectedErrorLogs, logger.errorLogs, "Should log error for terminal failure")
	assert.Empty(t, logger.infoLogs)

	// 2. Second call to Execute should attempt to send the task to the DLQ. In this case, it will fail, and we should
	// log the error and record a samples.
	err = dlqObserver.Execute()
	assert.ErrorIs(t, err, queues.ErrSendTaskToDLQ)
	snapshot = capture.Snapshot()
	verifySample(t, snapshot[metrics.TaskDLQFailures.GetMetricName()], int64(1))
	expectedErrorLogs = append(expectedErrorLogs, logRecord{
		msg:  "Failed to send history task to the DLQ",
		tags: append(expectedTags, tag.Error(err)),
	})
	verifyLogsEqual(
		t,
		expectedErrorLogs,
		logger.errorLogs,
		"Should log an error that we failed to send the task to the DLQ",
	)
	assert.Empty(t, logger.infoLogs)

	// 3. The third call to Execute should successfully send the task to the DLQ, recording the time it took from when
	// the task originally failed to when it was finally sent to the DLQ.
	dlq.EnqueueTaskErr = nil
	err = dlqObserver.Execute()
	assert.NoError(t, err)
	snapshot = capture.Snapshot()
	verifySample(t, snapshot[metrics.TaskDLQSendLatency.GetMetricName()], time.Second)
	verifyLogsEqual(t, expectedErrorLogs, logger.errorLogs)
	verifyLogsEqual(t, []logRecord{
		{
			msg:  "Task sent to DLQ",
			tags: expectedTags,
		},
	}, logger.infoLogs, "Should log when the task is finally successfully sent to the DLQ")
}

func verifySample(t *testing.T, samples []*metricstest.CapturedRecording, value any) {
	t.Helper()
	assert.Len(t, samples, 1)
	if len(samples) == 0 {
		return
	}
	sample := samples[0]
	assert.Equal(t, value, sample.Value)
	assert.Equal(t, "mock namespace name", sample.Tags["namespace"])
	assert.Equal(t, "TransferWorkflowTask", sample.Tags["task_type"])
}

func TestExecutableDLQObserver_GetNamespaceByIDErr(t *testing.T) {
	// Test that we gracefully handle errors getting the namespace name for a task.

	t.Parallel()

	ctrl := gomock.NewController(t)
	namespaceEntry := tests.GlobalNamespaceEntry
	task := &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			string(namespaceEntry.ID()),
			"test-workflow-id",
			"test-run-id",
		),
		TaskID: 42,
	}
	timeSource := clock.NewEventTimeSource()

	dlq := &queuestest.FakeDLQ{}
	logger := &testLogger{}
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	executable := queuestest.NewFakeExecutable(task, errTerminal)
	executableDLQ := newExecutableDLQ(executable, dlq)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	getNsErr := errors.New("some error getting namespace")
	namespaceRegistry.EXPECT().GetNamespaceByID(namespaceEntry.ID()).Return(nil, getNsErr)
	dlqObserver := queues.NewExecutableDLQObserver(executableDLQ, 1, namespaceRegistry, timeSource, logger, metricsHandler)
	dlq.EnqueueTaskErr = errors.New("some error writing to DLQ")

	err := dlqObserver.Execute()
	assert.ErrorIs(t, err, queues.ErrTerminalTaskFailure)
	snapshot := capture.Snapshot()
	metric := snapshot[metrics.TaskTerminalFailures.GetMetricName()]
	if assert.Len(t, metric, 1) {
		assert.Equal(t, int64(1), metric[0].Value)
		assert.Empty(t, metric[0].Tags["namespace"])
	}
	shardID := tasks.GetShardIDForTask(task, 1)
	expectedTags := []tag.Tag{
		tag.WorkflowNamespaceID(string(namespaceEntry.ID())),
		tag.WorkflowID("test-workflow-id"),
		tag.WorkflowRunID("test-run-id"),
		tag.ShardID(int32(shardID)),
		tag.TaskID(42),
	}
	expectedErrorLogs := []logRecord{
		{
			msg:  "Unable to get namespace",
			tags: append(expectedTags, tag.Error(getNsErr)),
		},
		{
			msg:  "A terminal error occurred while processing this task",
			tags: append(expectedTags, tag.Error(err)),
		},
	}
	verifyLogsEqual(t, expectedErrorLogs, logger.errorLogs)
	assert.Empty(t, logger.infoLogs)
}

// verifyLogsEqual verifies that the actual list of logs is the exact same as the expected list of logs, with the
// relaxed constraint that the actual list of logs may contain additional tags.
func verifyLogsEqual(t *testing.T, expected []logRecord, actual []logRecord, msgAndArgs ...interface{}) {
	t.Helper()
	if !assert.Len(t, actual, len(expected), msgAndArgs) {
		return
	}
	for i, expectedLog := range expected {
		actualLog := actual[i]
		assert.Equal(t, expectedLog.msg, actualLog.msg, msgAndArgs)
		for _, tg := range expectedLog.tags {
			assert.Contains(t, actualLog.tags, tg, msgAndArgs)
		}
	}
}
