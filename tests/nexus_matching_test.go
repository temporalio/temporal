package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
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

type NexusMatchingTestSuite struct {
	testcore.FunctionalTestBase
}

func TestNexusMatchingSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(NexusMatchingTestSuite))
}

func (s *NexusMatchingTestSuite) TestDispatchNexusTaskWithMatchingBehaviors() {
	for _, forcePollForward := range []bool{false, true} {
		for _, forceTaskForward := range []bool{false, true} {
			for _, forceAsync := range []bool{false, true} {
				name := "NoTaskForward"
				if forceTaskForward {
					name = "ForceTaskForward"
				}
				if forcePollForward {
					name += "ForcePollForward"
				} else {
					name += "NoPollForward"
				}
				if forceAsync {
					name += "ForceAsync"
				} else {
					name += "AllowSync"
				}

				s.Run(name, func() {
					if forceTaskForward || forcePollForward {
						s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 13)
						s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 13)
					} else {
						s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
						s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
					}
					if forceTaskForward {
						s.InjectHook(testhooks.MatchingLBForceWritePartition, 11)
					} else {
						s.InjectHook(testhooks.MatchingLBForceWritePartition, 0)
					}
					if forcePollForward {
						s.InjectHook(testhooks.MatchingLBForceReadPartition, 5)
					} else {
						s.InjectHook(testhooks.MatchingLBForceReadPartition, 0)
					}
					if forceAsync {
						s.InjectHook(testhooks.MatchingDisableSyncMatch, true)
					} else {
						s.InjectHook(testhooks.MatchingDisableSyncMatch, false)
					}

					s.dispatchAndCompleteNexusTask(forceTaskForward, forcePollForward)
				})
			}
		}
	}
}

func (s *NexusMatchingTestSuite) TestDispatchNexusTaskOnNonRootPartitionNoForwarding() {
	// Both poll and task go to partition 1 with no forwarding. This verifies that
	// non-root partitions work correctly even when no forwarding is involved.
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	s.OverrideDynamicConfig(dynamicconfig.MatchingForwarderMaxOutstandingTasks, 0) // disable forwarding
	s.OverrideDynamicConfig(dynamicconfig.MatchingForwarderMaxOutstandingPolls, 0) // disable forwarding
	s.InjectHook(testhooks.MatchingLBForceWritePartition, 1)
	s.InjectHook(testhooks.MatchingLBForceReadPartition, 1)
	s.InjectHook(testhooks.MatchingDisableSyncMatch, false)

	s.dispatchAndCompleteNexusTask(false, false)
}

func (s *NexusMatchingTestSuite) dispatchAndCompleteNexusTask(expectTaskForwarded, expectPollForwarded bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("test-nexus-tq")
	matchingClient := s.GetTestCluster().MatchingClient()

	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

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

	// Start polling for the nexus task via the frontend client in the background.
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

	// Dispatch the nexus task via the matching client.
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

	// Wait for the poll to return a task.
	var pollRes pollResult
	select {
	case pollRes = <-pollDone:
	case <-ctx.Done():
		s.FailNow("timed out waiting for poll to return a task")
	}
	s.NoError(pollRes.err)
	s.NotNil(pollRes.resp)
	s.NotEmpty(pollRes.resp.TaskToken)
	s.NotNil(pollRes.resp.Request)

	// Verify the dispatched request is what we sent.
	s.Equal(nexusRequest.GetStartOperation().GetService(), pollRes.resp.Request.GetStartOperation().GetService())
	s.Equal(nexusRequest.GetStartOperation().GetOperation(), pollRes.resp.Request.GetStartOperation().GetOperation())

	// Complete the task via the frontend client with a sync success response.
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
	s.NoError(err)

	// Wait for the dispatch caller to receive the completed result.
	var dispatchRes dispatchResult
	select {
	case dispatchRes = <-dispatchDone:
	case <-ctx.Done():
		s.FailNow("timed out waiting for dispatch result")
	}
	s.NoError(dispatchRes.err)
	s.NotNil(dispatchRes.resp)

	// Verify the dispatch response contains the completed result.
	response := dispatchRes.resp.GetResponse()
	s.NotNil(response, "expected dispatch response to contain a nexus response")
	syncSuccess := response.GetStartOperation().GetSyncSuccess()
	s.NotNil(syncSuccess, fmt.Sprintf("expected sync success response, got: %v", response))
	s.Equal(completionPayload.Data, syncSuccess.Payload.Data)

	// Verify forwarding behavior via metrics.
	s.verifyForwardingMetrics(capture, expectTaskForwarded, expectPollForwarded)
}

func (s *NexusMatchingTestSuite) verifyForwardingMetrics(
	capture *metricstest.Capture,
	expectTaskForwarded bool,
	expectPollForwarded bool,
) {
	snap := capture.Snapshot()

	// Verify forwarded metric has the expected operation
	dispatchRecordings := snap["forwarded"]
	foundExpectedTaskForward := false
	for _, rec := range dispatchRecordings {
		if rec.Tags["operation"] == "MatchingDispatchNexusTask" {
			foundExpectedTaskForward = true
			break
		}
	}
	s.Equal(expectTaskForwarded, foundExpectedTaskForward,
		"expected task forward mismatch, expected: %v, actual: %v", expectTaskForwarded, foundExpectedTaskForward)

	// Verify poll_latency has the expected "forwarded" and "task_type" tags.
	pollRecordings := snap["poll_latency"]
	s.NotEmpty(pollRecordings, "expected poll_latency metric to be recorded")
	foundExpectedPollForward := false
	for _, rec := range pollRecordings {
		if rec.Tags["forwarded"] == fmt.Sprintf("%v", expectPollForwarded) && rec.Tags["task_type"] == "Nexus" {
			foundExpectedPollForward = true
			break
		}
	}
	s.True(foundExpectedPollForward,
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
