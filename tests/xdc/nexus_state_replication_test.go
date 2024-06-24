// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package xdc

import (
	"context"
	"flag"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"

	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/tests"
)

type NexusStateReplicationSuite struct {
	xdcBaseSuite
}

func TestNexusStateReplicationTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(NexusStateReplicationSuite))
}

func (s *NexusStateReplicationSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key(): 1000,
		dynamicconfig.EnableNexus.Key():                  true,
		dynamicconfig.RefreshNexusEndpointsMinWait.Key(): 1 * time.Millisecond,
	}
	s.setupSuite([]string{"nexus_state_replication_active", "nexus_state_replication_standby"})
}

func (s *NexusStateReplicationSuite) SetupTest() {
	s.setupTest()
}

func (s *NexusStateReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestNexusOperationEventsReplicated tests that nexus related operation events are replicated across clusters and that
// the operation machinary functions as expected when failover happens.
// General outline:
// 1. Start two clusters, cluster1 set to active, cluster2 set to standby.
// 2. Start a workflow on cluster1.
// 3. Schedule a nexus operation on cluster1. The request is expected to fail due to missing dynamic config for the callback URL template.
// 4. Failover to cluster2.
// 5. Wait for the operation to be started on cluster2.
// 6. Fail back to cluster1.
// 7. Complete the operation via callback on cluster1.
// 8. Check that the operation completion triggers a workflow task when we poll on cluster1.
// 9. Complete the workflow.
func (s *NexusStateReplicationSuite) TestNexusOperationEventsReplicated() {
	var callbackToken string
	var publicCallbackUrl string

	h := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackUrl = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationID: "test"}, nil
		},
	}
	listenAddr := nexustest.AllocListenAddress(s.T())
	nexustest.NewNexusServer(s.T(), listenAddr, h)

	ctx := tests.NewContext()
	ns := s.createGlobalNamespace()

	// Set URL template after httpAPAddress is set, see commonnexus.RouteCompletionCallback.
	// But only on cluster2, this will prevent the operation request from starting in cluster1, we expect the operation
	// to be executed in cluster2 after failover is complete.
	s.cluster2.OverrideDynamicConfig(
		s.T(),
		nexusoperations.CallbackURLTemplate,
		// We'll send the callback to cluster1, when we fail back to it.
		"http://"+s.cluster1.GetHost().FrontendHTTPAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

	// Nexus endpoints registry isn't replicated yet, manually create the same endpoint in both clusters.
	for _, cl := range []operatorservice.OperatorServiceClient{s.cluster1.GetOperatorClient(), s.cluster2.GetOperatorClient()} {
		_, err := cl.CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
			Spec: &nexuspb.EndpointSpec{
				Name: "endpoint",
				Target: &nexuspb.EndpointTarget{
					Variant: &nexuspb.EndpointTarget_External_{
						External: &nexuspb.EndpointTarget_External{
							Url: "http://" + listenAddr,
						},
					},
				},
			},
		})
		s.NoError(err)
	}

	sdkClient1, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.GetHost().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)
	sdkClient2, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster2.GetHost().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)

	run, err := sdkClient1.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: "tq",
		ID:        "test",
	}, "workflow")
	s.NoError(err)

	pollRes := s.pollWorkflowTask(ctx, s.cluster1.GetFrontendClient(), ns)
	_, err = s.cluster1.GetFrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: pollRes.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  "endpoint",
						Service:   "service",
						Operation: "operation",
					},
				},
			},
		},
	})
	s.NoError(err)

	// Ensure the scheduled event is replicated.
	s.waitEvent(ctx, sdkClient2, run, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED)

	// Now failover, and let cluster2 be the active.
	s.failover(ns, s.clusterNames[1], 2, s.cluster1.GetFrontendClient())

	// Poll in cluster2 (previously standby) and verify the operation was started.
	pollRes = s.pollWorkflowTask(ctx, s.cluster2.GetFrontendClient(), ns)
	_, err = s.cluster2.GetFrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: pollRes.TaskToken,
		Commands:  []*commandpb.Command{}, // No need to generate other commands, this "workflow" just waits for the operation to complete.
	})
	s.NoError(err)
	idx := slices.IndexFunc(pollRes.History.Events, func(ev *historypb.HistoryEvent) bool {
		return ev.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED
	})
	s.Greater(idx, -1)

	// Ensure the started event is replicated back to cluster1.
	s.waitEvent(ctx, sdkClient1, run, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)

	// Fail back to cluster1.
	s.failover(ns, s.clusterNames[0], 11, s.cluster2.GetFrontendClient())

	s.completeNexusOperation(ctx, "result", publicCallbackUrl, callbackToken)

	// Verify completion triggers a new workflow task and that the workflow completes.
	pollRes = s.pollWorkflowTask(ctx, s.cluster1.GetFrontendClient(), ns)
	_, err = s.cluster1.GetFrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: pollRes.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
				},
			},
		},
	})
	s.NoError(err)
	idx = slices.IndexFunc(pollRes.History.Events, func(ev *historypb.HistoryEvent) bool {
		return ev.EventType == enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED
	})
	s.Greater(idx, -1)

	s.NoError(run.Get(ctx, nil))
}

func (s *NexusStateReplicationSuite) waitEvent(ctx context.Context, sdkClient sdkclient.Client, run sdkclient.WorkflowRun, eventType enumspb.EventType) {
	s.Eventually(func() bool {
		history := sdkClient.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for history.HasNext() {
			event, err := history.Next()
			s.NoError(err)
			if event.EventType == eventType {
				return true
			}
		}
		return false
	}, time.Second*10, time.Millisecond*100)
}

func (s *NexusStateReplicationSuite) pollWorkflowTask(ctx context.Context, client tests.FrontendClient, ns string) *workflowservice.PollWorkflowTaskQueueResponse {
	pollRes, err := client.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: ns,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "tq",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test",
	})
	s.NoError(err)
	return pollRes
}

func (s *NexusStateReplicationSuite) completeNexusOperation(ctx context.Context, result any, callbackUrl, callbackToken string) {
	completion, err := nexus.NewOperationCompletionSuccessful(s.mustToPayload(result), nexus.OperationCompletionSuccesfulOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(err)
	req, err := nexus.NewCompletionHTTPRequest(ctx, callbackUrl, completion)
	s.NoError(err)
	if callbackToken != "" {
		req.Header.Add(commonnexus.CallbackTokenHeader, callbackToken)
	}

	res, err := http.DefaultClient.Do(req)
	s.NoError(err)
	defer res.Body.Close()
	_, err = io.ReadAll(res.Body)
	s.NoError(err)
	s.Equal(http.StatusOK, res.StatusCode)
}
