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
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/tests"
)

var op = nexus.NewOperationReference[string, string]("my-operation")

type NexusRequestForwardingSuite struct {
	xdcBaseSuite
}

func TestNexusRequestForwardingTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(NexusRequestForwardingSuite))
}

func (s *NexusRequestForwardingSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]interface{}{
		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS: 1000,
		dynamicconfig.FrontendEnableNexusAPIs:                                    true,
		nexusoperations.Enabled:                                                  true,
		dynamicconfig.FrontendRefreshNexusIncomingServicesMinWait:                5 * time.Millisecond,
	}
	s.setupSuite([]string{"nexus_request_forwarding_active", "nexus_request_forwarding_standby"})
}

func (s *NexusRequestForwardingSuite) SetupTest() {
	s.setupTest()
}

func (s *NexusRequestForwardingSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *NexusRequestForwardingSuite) TestStartOperationForwardedFromStandbyToActive() {
	ctx := tests.NewContext()
	namespace := fmt.Sprintf("%v-%v", "test-namespace", uuid.New())
	taskQueue := fmt.Sprintf("%v-%v", "test-task-queue", uuid.New())
	serviceName := fmt.Sprintf("%v-%v", "test-service", uuid.New())

	activeFrontendClient := s.cluster1.GetFrontendClient()
	regReq := &workflowservice.RegisterNamespaceRequest{
		Namespace:                        namespace,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusterNames[0],
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * time.Hour * 24),
	}
	_, err := activeFrontendClient.RegisterNamespace(ctx, regReq)
	s.NoError(err)

	passiveFrontendClient := s.cluster2.GetFrontendClient()
	s.EventuallyWithT(func(t *assert.CollectT) {
		// Wait for namespace record to be replicated.
		_, err := passiveFrontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
		assert.NoError(t, err)
	}, 15*time.Second, 500*time.Millisecond)

	activeOperatorClient := s.cluster1.GetOperatorClient()
	_, err = activeOperatorClient.CreateNexusIncomingService(ctx, &operatorservice.CreateNexusIncomingServiceRequest{
		Spec: &nexuspb.IncomingServiceSpec{
			Name:      serviceName,
			Namespace: namespace,
			TaskQueue: taskQueue,
		},
	})
	s.NoError(err)

	passiveOperatorClient := s.cluster2.GetOperatorClient()
	s.EventuallyWithT(func(t *assert.CollectT) {
		_, err = passiveOperatorClient.CreateNexusIncomingService(ctx, &operatorservice.CreateNexusIncomingServiceRequest{
			Spec: &nexuspb.IncomingServiceSpec{
				Name:      serviceName,
				Namespace: namespace,
				TaskQueue: taskQueue,
			},
		})
		assert.NoError(t, err)
	}, 15*time.Second, 500*time.Millisecond)

	dispatchURL := fmt.Sprintf("http://%s/%s", s.cluster2.GetHost().FrontendHTTPAddress(), cnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Path(cnexus.NamespaceAndTaskQueue{Namespace: namespace, TaskQueue: taskQueue}))
	nexusClient, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: dispatchURL})
	s.NoError(err)

	activeMetricsHandler := s.cluster1.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
	activeCapture := activeMetricsHandler.StartCapture()
	defer activeMetricsHandler.StopCapture(activeCapture)

	passiveMetricsHandler := s.cluster2.GetHost().GetMetricsHandler().(*metricstest.CaptureHandler)
	passiveCapture := passiveMetricsHandler.StartCapture()
	defer passiveMetricsHandler.StopCapture(passiveCapture)

	go s.nexusTaskPoller(activeFrontendClient, namespace, taskQueue)

	var startResult *nexus.ClientStartOperationResult[string]
	s.EventuallyWithT(func(t *assert.CollectT) {
		startResult, err = nexus.StartOperation(ctx, nexusClient, op, "input", nexus.StartOperationOptions{
			CallbackURL: "http://localhost/callback",
			RequestID:   "request-id",
			Header:      nexus.Header{"key": "value"},
		})
		assert.NoError(t, err)
	}, 15*time.Second, 500*time.Millisecond)
	s.Equal("input", startResult.Successful)

	activeSnap := activeCapture.Snapshot()
	s.Equal(1, len(activeSnap["nexus_requests"]))
	s.Subset(activeSnap["nexus_requests"][0].Tags, map[string]string{"namespace": namespace, "method": "StartOperation", "outcome": "sync_success"})
	s.Contains(activeSnap["nexus_requests"][0].Tags, "service")
	s.Equal(int64(1), activeSnap["nexus_requests"][0].Value)
	s.Equal(metrics.MetricUnit(""), activeSnap["nexus_requests"][0].Unit)
	s.Equal(1, len(activeSnap["nexus_latency"]))
	s.Subset(activeSnap["nexus_latency"][0].Tags, map[string]string{"namespace": namespace, "method": "StartOperation", "outcome": "sync_success"})
	s.Contains(activeSnap["nexus_latency"][0].Tags, "service")
	s.Equal(metrics.MetricUnit(metrics.Milliseconds), activeSnap["nexus_latency"][0].Unit)

	passiveSnap := passiveCapture.Snapshot()
	s.Equal(1, len(passiveSnap["nexus_requests"]))
	s.Subset(passiveSnap["nexus_requests"][0].Tags, map[string]string{"namespace": namespace, "method": "StartOperation", "outcome": "request_forwarded"})
	s.Contains(passiveSnap["nexus_requests"][0].Tags, "service")
	s.Equal(int64(1), passiveSnap["nexus_requests"][0].Value)
	s.Equal(metrics.MetricUnit(""), passiveSnap["nexus_requests"][0].Unit)
	s.Equal(1, len(passiveSnap["nexus_latency"]))
	s.Subset(passiveSnap["nexus_latency"][0].Tags, map[string]string{"namespace": namespace, "method": "StartOperation", "outcome": "request_forwarded"})
	s.Contains(passiveSnap["nexus_latency"][0].Tags, "service")
	s.Equal(metrics.MetricUnit(metrics.Milliseconds), passiveSnap["nexus_latency"][0].Unit)
}

func (s *NexusRequestForwardingSuite) TestCancelOperationForwardedFromStandbyToActive() {

}

func (s *NexusRequestForwardingSuite) nexusTaskPoller(frontendClient tests.FrontendClient, namespace string, taskQueue string) {
	ctx := tests.NewContext()
	res, err := frontendClient.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: namespace,
		Identity:  uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
	})
	s.NoError(err)

	_, err = frontendClient.RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: namespace,
		Identity:  uuid.NewString(),
		TaskToken: res.TaskToken,
		Response: &nexuspb.Response{
			Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_SyncSuccess{
						SyncSuccess: &nexuspb.StartOperationResponse_Sync{
							Payload: res.Request.GetStartOperation().GetPayload()}}}},
		},
	})
	s.NoError(err)
}
