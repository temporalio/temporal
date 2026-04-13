package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/tests/testcore"
)

func TestDispatchNexusTaskWithMatchingBehaviors(t *testing.T) {
	t.Parallel()
	runWithMatchingBehaviors(t, nil, func(s *testcore.TestEnv, b testcore.MatchingBehavior) {
		dispatchAndCompleteNexusTask(t, s, b.ForceTaskForward, b.ForcePollForward)
	})
}

func TestDispatchNexusTaskOnNonRootPartitionNoForwarding(t *testing.T) {
	// Both poll and task go to partition 1 with no forwarding. This verifies that
	// non-root partitions work correctly even when no forwarding is involved.
	s := testcore.NewEnv(t, testcore.WithDedicatedCluster())

	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingForwarderMaxOutstandingTasks, 0) // disable forwarding
	s.OverrideDynamicConfig(dynamicconfig.MatchingForwarderMaxOutstandingPolls, 0) // disable forwarding
	s.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceWritePartition, 1))
	s.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceReadPartition, 1))
	s.InjectHook(testhooks.NewHook(testhooks.MatchingDisableSyncMatch, false))

	dispatchAndCompleteNexusTask(t, s, false, false)
}

func dispatchAndCompleteNexusTask(t *testing.T, s *testcore.TestEnv, expectTaskForwarded, expectPollForwarded bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("test-nexus-tq")
	matchingClient := s.GetTestCluster().MatchingClient()

	capture := s.StartNamespaceMetricCapture()

	nexusRequest := &nexuspb.Request{
		Header: map[string]string{
			"test-header-key": "test-header-value",
		},
		Variant: &nexuspb.Request_StartOperation{
			StartOperation: &nexuspb.StartOperationRequest{
				Service:   "test-service",
				Operation: "test-operation",
				RequestId: uuid.NewString(),
			},
		},
	}

	type dispatchResult struct {
		resp *matchingservice.DispatchNexusTaskResponse
		err  error
	}
	dispatchDone := make(chan dispatchResult, 1)

	type pollResult struct {
		resp *workflowservice.PollNexusTaskQueueResponse
		err  error
	}
	pollDone := make(chan pollResult, 1)
	go func() {
		resp, err := s.FrontendClient().PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: s.Namespace().String(),
			Identity:  "test-worker",
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
		})
		pollDone <- pollResult{resp: resp, err: err}
	}()

	go func() {
		resp, err := matchingClient.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
			NamespaceId: s.NamespaceID().String(),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Request: nexusRequest,
		})
		dispatchDone <- dispatchResult{resp: resp, err: err}
	}()

	var pollRes pollResult
	select {
	case pollRes = <-pollDone:
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for poll to return a task")
	}
	require.NoError(t, pollRes.err)
	require.NotNil(t, pollRes.resp)
	require.NotEmpty(t, pollRes.resp.TaskToken)
	require.NotNil(t, pollRes.resp.Request)

	require.Equal(t, nexusRequest.GetStartOperation().GetService(), pollRes.resp.Request.GetStartOperation().GetService())
	require.Equal(t, nexusRequest.GetStartOperation().GetOperation(), pollRes.resp.Request.GetStartOperation().GetOperation())

	completionPayload := &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     []byte(`"nexus-result"`),
	}
	_, err := s.FrontendClient().RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		Identity:  "test-worker",
		TaskToken: pollRes.resp.TaskToken,
		Response: &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_SyncSuccess{
						SyncSuccess: &nexuspb.StartOperationResponse_Sync{
							Payload: completionPayload,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	var dispatchRes dispatchResult
	select {
	case dispatchRes = <-dispatchDone:
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for dispatch result")
	}
	require.NoError(t, dispatchRes.err)
	require.NotNil(t, dispatchRes.resp)

	response := dispatchRes.resp.GetResponse()
	require.NotNil(t, response, "expected dispatch response to contain a nexus response")
	syncSuccess := response.GetStartOperation().GetSyncSuccess()
	require.NotNil(t, syncSuccess, "expected sync success response, got: %v", response)
	require.Equal(t, completionPayload.Data, syncSuccess.Payload.Data)

	verifyForwardingMetrics(t, capture, expectTaskForwarded, expectPollForwarded)
}

func verifyForwardingMetrics(t *testing.T, capture *testcore.NamespaceMetricCapture, expectTaskForwarded bool, expectPollForwarded bool) {
	dispatchRecordings := capture.Metric("forwarded")
	foundExpectedTaskForward := false
	for _, rec := range dispatchRecordings {
		if rec.Tags["operation"] == "MatchingDispatchNexusTask" {
			foundExpectedTaskForward = true
			break
		}
	}
	require.Equal(t, expectTaskForwarded, foundExpectedTaskForward,
		"expected task forward mismatch, expected: %v, actual: %v", expectTaskForwarded, foundExpectedTaskForward)

	pollRecordings := capture.Metric("poll_latency")
	require.NotEmpty(t, pollRecordings, "expected poll_latency metric to be recorded")
	foundExpectedPollForward := false
	for _, rec := range pollRecordings {
		if rec.Tags["forwarded"] == fmt.Sprintf("%v", expectPollForwarded) && rec.Tags["task_type"] == "Nexus" {
			foundExpectedPollForward = true
			break
		}
	}
	require.True(t, foundExpectedPollForward,
		"expected poll_latency with forwarded=%v and task_type=Nexus, recordings: %v",
		expectPollForwarded, formatRecordings(pollRecordings))
}

func formatRecordings(recordings []*metricstest.CapturedRecording) string {
	var result string
	for i, rec := range recordings {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("{forwarded=%s, task_type=%s}", rec.Tags["forwarded"], rec.Tags["task_type"])
	}
	return "[" + result + "]"
}
