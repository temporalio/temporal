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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
)

type versioningIntegSuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	IntegrationBase
	sdkClient sdkclient.Client
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

	clientAddr := "127.0.0.1:7134"
	if TestFlags.FrontendAddr != "" {
		clientAddr = TestFlags.FrontendAddr
	}
	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  clientAddr,
		Namespace: s.namespace,
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sdkClient = sdkClient
}

func (s *versioningIntegSuite) TearDownTest() {
	s.sdkClient.Close()
}

func TestVersioningIntegrationSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(versioningIntegSuite))
}

func (s *versioningIntegSuite) TestBasicVersionUpdate() {
	ctx := NewContext()
	tq := "integration-versioning-basic"

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
	s.Equal("foo-2", res2.CompatibleLeaves[0].GetPreviousCompatible().GetVersion().GetWorkerBuildId())
}

func (s *versioningIntegSuite) TestLinkToNonexistentCompatibleVersionReturnsNotFound() {
	ctx := NewContext()
	tq := "integration-versioning-compat-not-found"

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

// This test verifies that the lease renewal of a task queue does not destroy the versioning data - as it updates the
// task queue info. We need to make sure that update preserves the versioning data.
func (s *versioningIntegSuite) TestVersioningStateNotDestroyedByOtherUpdates() {
	ctx := NewContext()
	tq := "integration-versioning-not-destroyed"

	res, err := s.engine.UpdateWorkerBuildIdOrdering(ctx, &workflowservice.UpdateWorkerBuildIdOrderingRequest{
		Namespace:          s.namespace,
		TaskQueue:          tq,
		VersionId:          &taskqueuepb.VersionId{WorkerBuildId: "foo"},
		PreviousCompatible: nil,
		BecomeDefault:      true,
	})
	s.NoError(err)
	s.NotNil(res)

	sdkWorker := worker.New(s.sdkClient, tq, worker.Options{})
	if err := sdkWorker.Start(); err != nil {
		s.Logger.Fatal("Error starting worker", tag.Error(err))
	}

	wfFunc := func(ctx workflow.Context) error {
		// This timer exists to ensure the lease-renewal on the task queue happens, to verify that doesn't blow up data.
		// The renewal interval has been lowered in this suite.
		_ = workflow.Sleep(ctx, 3*time.Second)
		return nil
	}
	sdkWorker.RegisterWorkflow(wfFunc)
	id := "integration-test-unhandled-command-new-task"
	workflowOptions := sdkclient.StartWorkflowOptions{ID: id, TaskQueue: tq}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, wfFunc)
	s.NoError(err)
	err = workflowRun.Get(ctx, nil)
	s.NoError(err)
	sdkWorker.Stop()

	res2, err := s.engine.GetWorkerBuildIdOrdering(ctx, &workflowservice.GetWorkerBuildIdOrderingRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal("foo", res2.CurrentDefault.GetVersion().GetWorkerBuildId())
}
