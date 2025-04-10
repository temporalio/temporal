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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type PollerScalingIntegSuite struct {
	testcore.FunctionalTestSuite
	sdkClient sdkclient.Client
}

func (s *PollerScalingIntegSuite) mustToPayload(v any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(v)
	s.NoError(err)
	return payload
}

func TestPollerScalingFunctionalSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PollerScalingIntegSuite))
}

func (s *PollerScalingIntegSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		// Force one partition so we can reliably see the backlog
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():     1,
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key():    1,
		dynamicconfig.MatchingPollerScalingBacklogAgeScaleUp.Key(): 50 * time.Millisecond,
	}
	s.FunctionalTestBase.SetupSuiteWithDefaultCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *PollerScalingIntegSuite) SetupTest() {
	s.FunctionalTestSuite.SetupTest()

	var err error
	s.sdkClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
}

func (s *PollerScalingIntegSuite) TearDownTest() {
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
}

func (s *PollerScalingIntegSuite) TestPollerScalingSimpleBacklog() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	tq := testcore.RandomizeStr(s.T().Name())
	endpointName := testcore.RandomizedNexusEndpoint(s.T().Name())
	s.OverrideDynamicConfig(
		nexusoperations.CallbackURLTemplate,
		"http://"+s.HttpAPIAddress()+"/namespaces/{{.NamespaceName}}/nexus/callback")

	_, err := s.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: s.Namespace().String(),
						TaskQueue: tq,
					},
				},
			},
		},
	})
	s.NoError(err)

	// Queue up a couple workflows
	for i := 0; i < 5; i++ {
		_, err := s.sdkClient.ExecuteWorkflow(
			ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
		s.NoError(err)
	}

	// Poll for a task and see attached decision is to scale up b/c of backlog
	feClient := s.FrontendClient()
	// This needs to be done in an eventually loop because nexus endpoints don't become available immediately...
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := feClient.PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		assert.NoError(t, err)
		if assert.NotNil(t, resp.PollerScalingDecision) {
			assert.GreaterOrEqual(t, int32(1), resp.PollerScalingDecision.PollRequestDeltaSuggestion)
		}

		// Start enough activities / nexus tasks to ensure we will see scale up decisions
		commands := make([]*commandpb.Command, 0, 5)
		for i := 0; i < 5; i++ {
			commands = append(commands, &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId:          fmt.Sprintf("%v", i),
					ActivityType:        &commonpb.ActivityType{Name: "test-activity-type"},
					TaskQueue:           &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:               payloads.EncodeString("test-input"),
					StartToCloseTimeout: durationpb.New(10 * time.Second),
				}},
			})
			commands = append(commands, &commandpb.Command{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:  endpointName,
						Service:   "service",
						Operation: "operation",
						Input:     s.mustToPayload("input"),
					},
				},
			},
			)
		}
		_, err = feClient.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: resp.TaskToken,
			Commands:  commands,
		})
		assert.NoError(t, err)
	}, 20*time.Second, 200*time.Millisecond)

	// Wait to ensure add rate exceeds dispatch rate & backlog age grows
	tqtyp := enumspb.TASK_QUEUE_TYPE_ACTIVITY
	s.EventuallyWithT(func(t *assert.CollectT) {
		res, err := feClient.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:      s.Namespace().String(),
			TaskQueue:      &taskqueuepb.TaskQueue{Name: tq},
			ApiMode:        enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			TaskQueueTypes: []enumspb.TaskQueueType{tqtyp},
			ReportStats:    true,
		})
		assert.NoError(t, err)
		stats := res.GetVersionsInfo()[""].TypesInfo[int32(tqtyp)].Stats
		assert.GreaterOrEqual(t, stats.ApproximateBacklogAge.AsDuration(), 200*time.Millisecond)
	}, 20*time.Second, 200*time.Millisecond)

	actResp, err := feClient.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)
	s.NotNil(actResp.PollerScalingDecision)
	s.Assert().GreaterOrEqual(int32(1), actResp.PollerScalingDecision.PollRequestDeltaSuggestion)

	nexusResp, err := feClient.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	s.NoError(err)
	s.NotNil(nexusResp.PollerScalingDecision)
}

// Here we verify that, even with multiple partitions, pollers see scaling decisions at least often enough
// that SDKs have information to work with. Benchmark style testing that exists on the SDK side is better at ensuring
// that the desired outcomes actually happen.
func (s *PollerScalingIntegSuite) TestPollerScalingDecisionsAreSeenProbabilistically() {
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 5)
	s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 5)

	longctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	ctx, startWfCancel := context.WithCancel(longctx)
	tq := testcore.RandomizeStr(s.T().Name())

	// Fire off workflows until polling stops
	go func() {
		for {
			_, _ = s.sdkClient.ExecuteWorkflow(
				longctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
			select {
			case <-ctx.Done():
				return
			case <-time.NewTimer(200 * time.Millisecond).C:
				continue
			}
		}
	}()

	allScaleDecisions := make([]*taskqueuepb.PollerScalingDecision, 0, 15)
	for i := 0; i < 15; i++ {
		resp, _ := s.FrontendClient().PollWorkflowTaskQueue(longctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		if resp != nil {
			allScaleDecisions = append(allScaleDecisions, resp.PollerScalingDecision)
		}
		// Poll slightly less frequently than we insert tasks
		time.Sleep(300 * time.Millisecond) //nolint:forbidigo
	}
	startWfCancel()

	// We must have seen at least a handful of non-nil scaling decisions
	nonNilDecisions := util.FilterSlice(allScaleDecisions, func(d *taskqueuepb.PollerScalingDecision) bool { return d != nil })
	s.Assert().GreaterOrEqual(len(nonNilDecisions), 3)
}
