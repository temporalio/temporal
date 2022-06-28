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

package host

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives/timestamp"
)

type versioningIntegSuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	IntegrationBase
}

func (s *versioningIntegSuite) SetupSuite() {
	s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	s.dynamicConfigOverrides[dynamicconfig.MatchingUpdateAckInterval] = 1 * time.Second
	s.setupSuite("testdata/integration_test_cluster.yaml")
}

func (s *versioningIntegSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *versioningIntegSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
}

func TestVersioningIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(versioningIntegSuite))
}

func (s *versioningIntegSuite) TestBasicVersionUpdate() {
	ctx := NewContext()
	tq := "integration-versioning-basic"
	s.prepareQueue(ctx, tq)

	res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Namespace:          s.namespace,
		TaskQueue:          tq,
		VersionId:          &taskqueuepb.VersionId{WorkerBuildId: "foo"},
		PreviousCompatible: nil,
		BecomeDefault:      true,
	})
	s.NoError(err)
	s.NotNil(res)

	res2, err := s.engine.GetWorkerBuildIdOrdering(ctx, &workflowservice.GetWorkerBuildIdOrderingRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal("foo", res2.CurrentDefault.GetVersion().GetWorkerBuildId())
}

func (s *versioningIntegSuite) TestSeriesOfUpdates() {
	ctx := NewContext()
	tq := "integration-versioning-series"
	s.prepareQueue(ctx, tq)

	for i := 0; i < 10; i++ {
		res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
			Namespace:          s.namespace,
			TaskQueue:          tq,
			VersionId:          &taskqueuepb.VersionId{WorkerBuildId: fmt.Sprintf("foo-%d", i)},
			PreviousCompatible: nil,
			BecomeDefault:      true,
		})
		s.NoError(err)
		s.NotNil(res)
	}
	res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Namespace:          s.namespace,
		TaskQueue:          tq,
		VersionId:          &taskqueuepb.VersionId{WorkerBuildId: "foo-2.1"},
		PreviousCompatible: &taskqueuepb.VersionId{WorkerBuildId: "foo-2"},
		BecomeDefault:      false,
	})
	s.NoError(err)
	s.NotNil(res)

	res2, err := s.engine.GetWorkerBuildIdOrdering(ctx, &workflowservice.GetWorkerBuildIdOrderingRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal("foo-9", res2.CurrentDefault.GetVersion().GetWorkerBuildId())
	s.Equal(1, len(res2.CompatibleLeaves))
	s.Equal("foo-2.1", res2.CompatibleLeaves[0].GetVersion().GetWorkerBuildId())
}

func (s *versioningIntegSuite) TestLinkToNonexistentCompatibleVersionReturnsNotFound() {
	ctx := NewContext()
	tq := "integration-versioning-compat-not-found"
	s.prepareQueue(ctx, tq)

	res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Namespace:          s.namespace,
		TaskQueue:          tq,
		VersionId:          &taskqueuepb.VersionId{WorkerBuildId: "foo"},
		PreviousCompatible: &taskqueuepb.VersionId{WorkerBuildId: "i don't exist yo"},
	})
	s.Error(err)
	s.Nil(res)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *versioningIntegSuite) TestVersioningStateNotDestroyedByOtherUpdates() {
	ctx := NewContext()
	tq := "integration-versioning-not-destroyed"
	s.prepareQueue(ctx, tq)

	res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Namespace:          s.namespace,
		TaskQueue:          tq,
		VersionId:          &taskqueuepb.VersionId{WorkerBuildId: "foo"},
		PreviousCompatible: nil,
		BecomeDefault:      true,
	})
	s.NoError(err)
	s.NotNil(res)

	isFirst := true
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType,
		previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		// This timer exists to ensure the lease-renewal on the task queue happens, to verify that doesn't blow up data.
		// The renewal interval has been lowered in this suite.
		if isFirst {
			isFirst = false
			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            "timer-id-1",
					StartToFireTimeout: timestamp.DurationPtr(3 * time.Second),
				}},
			}}, nil
		}
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}}}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tq},
		Identity:            "whatever",
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: nil,
		Logger:              s.Logger,
		T:                   s.T(),
	}
	_, errWorkflowTask := poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(errWorkflowTask)
	_, errWorkflowTask = poller.PollAndProcessWorkflowTask(true, false)
	s.NoError(errWorkflowTask)

	res2, err := s.engine.GetWorkerBuildIdOrdering(ctx, &workflowservice.GetWorkerBuildIdOrderingRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal("foo", res2.CurrentDefault.GetVersion().GetWorkerBuildId())
}

func (s *versioningIntegSuite) prepareQueue(ctx context.Context, tq string) {
	workflowID := "integration-versioning-queuemaker"
	wt := "integration-versioning-queuemaker"
	identity := "worker1"

	// Make sure the task queue exists by starting a workflow on it
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:             uuid.New(),
		Namespace:             s.namespace,
		WorkflowId:            workflowID,
		WorkflowType:          &commonpb.WorkflowType{Name: wt},
		TaskQueue:             &taskqueuepb.TaskQueue{Name: tq},
		Input:                 nil,
		WorkflowRunTimeout:    timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout:   timestamp.DurationPtr(1 * time.Second),
		Identity:              identity,
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
	}

	// start workflow task, to ensure that the task queue exists
	// TODO: Should not be necessary - see https://github.com/temporalio/temporal/issues/2969
	we, err := s.engine.StartWorkflowExecution(ctx, request)
	s.NoError(err)
	s.Logger.Info("StartWorkflowExecution", tag.WorkflowRunID(we.RunId))
	_, err1 := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq},
		Identity:  identity,
	})
	s.NoError(err1)
}
