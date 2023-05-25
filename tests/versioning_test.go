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
	"errors"
	"flag"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/tqname"
)

type versioningIntegSuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	IntegrationBase
	sdkClient sdkclient.Client
}

const (
	partitionTreeDegree = 3
)

func (s *versioningIntegSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs:     true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs: true,
		dynamicconfig.MatchingForwarderMaxChildrenPerNode:        partitionTreeDegree,
	}
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

	s.addNewDefaultBuildId(ctx, tq, "foo")

	res2, err := s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal("foo", getCurrentDefault(res2))
}

func (s *versioningIntegSuite) TestSeriesOfUpdates() {
	ctx := NewContext()
	tq := "integration-versioning-series"

	for i := 0; i < 10; i++ {
		s.addNewDefaultBuildId(ctx, tq, fmt.Sprintf("foo-%d", i))
	}
	res, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				NewBuildId:                "foo-2.1",
				ExistingCompatibleBuildId: "foo-2",
				MakeSetDefault:            false,
			},
		},
	})
	s.NoError(err)
	s.NotNil(res)

	res2, err := s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal("foo-9", getCurrentDefault(res2))
	s.Equal("foo-2.1", res2.GetMajorVersionSets()[2].GetBuildIds()[1])
	s.Equal("foo-2", res2.GetMajorVersionSets()[2].GetBuildIds()[0])
}

func (s *versioningIntegSuite) TestLinkToNonexistentCompatibleVersionReturnsNotFound() {
	ctx := NewContext()
	tq := "integration-versioning-compat-not-found"

	res, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				NewBuildId:                "foo",
				ExistingCompatibleBuildId: "i don't exist yo",
			},
		},
	})
	s.Error(err)
	s.Nil(res)
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *versioningIntegSuite) TestVersioningStatePersistsAcrossUnload() {
	ctx := NewContext()
	tq := "integration-versioning-not-destroyed"

	s.addNewDefaultBuildId(ctx, tq, "foo")

	// Unload task queue to make sure the data is there when we load it again.
	_, err := s.testCluster.host.matchingClient.ForceUnloadTaskQueue(ctx, &matchingservice.ForceUnloadTaskQueueRequest{
		NamespaceId:   s.getNamespaceID(s.namespace),
		TaskQueue:     tq,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})

	res, err := s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res)
	s.Equal("foo", getCurrentDefault(res))
}

func (s *versioningIntegSuite) TestVersioningChangesPropagate() {
	ctx := NewContext()
	tq := "integration-versioning-propagate"

	// ensure at least two hops
	const partCount = 1 + partitionTreeDegree + partitionTreeDegree*partitionTreeDegree

	dc := s.testCluster.host.dcClient
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, partCount)
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, partCount)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueWritePartitions)

	for _, buildId := range []string{"foo", "foo-v2", "foo-v3"} {
		s.addNewDefaultBuildId(ctx, tq, buildId)
		s.waitForPropagation(ctx, tq, buildId)
	}
}

func (s *versioningIntegSuite) testWithMatchingBehavior(subtest func()) {
	dc := s.testCluster.host.dcClient
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueWritePartitions)
	defer dc.RemoveOverride(dynamicconfig.TestMatchingLBForceReadPartition)
	defer dc.RemoveOverride(dynamicconfig.TestMatchingLBForceWritePartition)
	defer dc.RemoveOverride(dynamicconfig.TestMatchingDisableSyncMatch)
	for _, forceForward := range []bool{false, true} {
		for _, forceAsync := range []bool{false, true} {
			name := ""
			if forceForward {
				// force two levels of forwarding
				dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 13)
				dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 13)
				dc.OverrideValue(dynamicconfig.TestMatchingLBForceReadPartition, 5)
				dc.OverrideValue(dynamicconfig.TestMatchingLBForceWritePartition, 11)
				name += "ForceForward"
			} else {
				// force single partition
				dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
				dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
				name += "NoForward"
			}
			if forceAsync {
				// disallow sync match to force to db
				dc.OverrideValue(dynamicconfig.TestMatchingDisableSyncMatch, true)
				name += "ForceAsync"
			} else {
				// default value
				dc.OverrideValue(dynamicconfig.TestMatchingDisableSyncMatch, false)
				name += "AllowSync"
			}
			s.Run(name, subtest)
		}
	}
}

func (s *versioningIntegSuite) TestDispatchNewWorkflow() {
	s.testWithMatchingBehavior(s.dispatchNewWorkflow)
}

func (s *versioningIntegSuite) dispatchNewWorkflow() {
	tq := s.randomizeStr(s.T().Name())

	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, "v1")
	s.waitForPropagation(ctx, tq, "v1")

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v1",
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *versioningIntegSuite) TestDispatchNewWorkflowStartWorkerFirst() {
	s.testWithMatchingBehavior(s.dispatchNewWorkflowStartWorkerFirst)
}

func (s *versioningIntegSuite) dispatchNewWorkflowStartWorkerFirst() {
	tq := s.randomizeStr(s.T().Name())

	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	// run worker before registering build. it will use guessed set id
	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v1",
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	// wait for it to start polling
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, "v1")
	s.waitForPropagation(ctx, tq, "v1")

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *versioningIntegSuite) TestDispatchUnversionedRemainsUnversioned() {
	s.testWithMatchingBehavior(s.dispatchUnversionedRemainsUnversioned)
}

func (s *versioningIntegSuite) dispatchUnversionedRemainsUnversioned() {
	tq := s.randomizeStr(s.T().Name())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	started := make(chan struct{}, 1)

	wf := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return "done!", nil
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		// no build id
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)

	s.waitForChan(ctx, started)
	s.addNewDefaultBuildId(ctx, tq, "v1")
	s.waitForPropagation(ctx, tq, "v1")

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *versioningIntegSuite) TestDispatchUpgradeStickyTimeout() {
	s.testWithMatchingBehavior(func() { s.dispatchUpgrade(true) })
}
func (s *versioningIntegSuite) TestDispatchUpgradeStickyUnavailable() {
	s.testWithMatchingBehavior(func() { s.dispatchUpgrade(false) })
}

func (s *versioningIntegSuite) dispatchUpgrade(letStickyWftTimeout bool) {
	tq := s.randomizeStr(s.T().Name())

	started := make(chan struct{}, 1)

	wf1 := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return "done!", nil
	}

	wf2 := func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return "done from two!", nil
	}

	// TODO: reduce after fixing sticky
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, "v1")
	s.waitForPropagation(ctx, tq, "v1")

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v1",
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	s.waitForChan(ctx, started)

	// Stop w1 to break stickiness
	// TODO: this shouldn't be necessary, add behavior cases that disable stickiness
	w1.Stop()

	// two methods of breaking stickiness:
	if letStickyWftTimeout {
		// in this case we just start the new worker and kick the workflow immediately. the new
		// wft will go to the sticky queue, be spooled, but eventually timeout and we'll get a
		// new wft.
	} else {
		// in this case we sleep for more than stickyPollerUnavailableWindow. matching will
		// return StickyWorkerUnavailable immediately after that.
		time.Sleep(11 * time.Second)
	}

	// now add v2 as compatible so the next workflow task runs there
	s.addCompatibleBuildId(ctx, tq, "v2", "v1", false)
	s.waitForPropagation(ctx, tq, "v2")

	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v2",
		UseBuildIDForVersioning: true,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done from two!", out)
}

func (s *versioningIntegSuite) TestDispatchActivity() {
	s.testWithMatchingBehavior(s.dispatchActivity)
}

func (s *versioningIntegSuite) dispatchActivity() {
	// This also implicitly tests that a workflow stays on a compatible version set if a new
	// incompatible set is registered, because wf2 just panics. It further tests that
	// stickiness on v1 is not broken by registering v2, because the channel send will panic on
	// replay after we close the channel.

	tq := s.randomizeStr(s.T().Name())

	started := make(chan struct{}, 1)

	act1 := func() (string, error) { return "v1", nil }
	act2 := func() (string, error) { return "v2", nil }
	wf1 := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		// wait for signal
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		// run two activities
		fut1 := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			DisableEagerExecution:  true,
			VersioningIntent:       worker.CompatibleVersion,
		}), "act")
		fut2 := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			DisableEagerExecution:  true,
			VersioningIntent:       worker.UseDefaultVersion, // this one should go to latest
		}), "act")
		var val1, val2 string
		s.NoError(fut1.Get(ctx, &val1))
		s.NoError(fut2.Get(ctx, &val2))
		return val1 + val2, nil
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		panic("workflow should not run on v2")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, "v1")
	s.waitForPropagation(ctx, tq, "v1")

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v1",
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started)
	close(started) //force panic if replayed

	// now register v2 as default
	s.addNewDefaultBuildId(ctx, tq, "v2")
	s.waitForPropagation(ctx, tq, "v2")
	// start worker for v2
	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v2",
		UseBuildIDForVersioning: true,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("v1v2", out)
}

func (s *versioningIntegSuite) TestDispatchChildWorkflow() {
	s.testWithMatchingBehavior(s.dispatchChildWorkflow)
}

func (s *versioningIntegSuite) dispatchChildWorkflow() {
	// This also implicitly tests that a workflow stays on a compatible version set if a new
	// incompatible set is registered, because wf2 just panics. It further tests that
	// stickiness on v1 is not broken by registering v2, because the channel send will panic on
	// replay after we close the channel.

	tq := s.randomizeStr(s.T().Name())

	started := make(chan struct{}, 1)

	child1 := func(workflow.Context) (string, error) { return "v1", nil }
	child2 := func(workflow.Context) (string, error) { return "v2", nil }
	wf1 := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		// wait for signal
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		// run two child workflows
		fut1 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{}), "child")
		fut2 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			VersioningIntent: worker.UseDefaultVersion, // this one should go to latest
		}), "child")
		var val1, val2 string
		s.NoError(fut1.Get(ctx, &val1))
		s.NoError(fut2.Get(ctx, &val2))
		return val1 + val2, nil
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		panic("workflow should not run on v2")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, "v1")
	s.waitForPropagation(ctx, tq, "v1")

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v1",
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterWorkflowWithOptions(child1, workflow.RegisterOptions{Name: "child"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started)
	close(started) //force panic if replayed

	// now register v2 as default
	s.addNewDefaultBuildId(ctx, tq, "v2")
	s.waitForPropagation(ctx, tq, "v2")
	// start worker for v2
	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v2",
		UseBuildIDForVersioning: true,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterWorkflowWithOptions(child2, workflow.RegisterOptions{Name: "child"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("v1v2", out)
}

func (s *versioningIntegSuite) TestDispatchContinueAsNew() {
	s.testWithMatchingBehavior(s.dispatchContinueAsNew)
}

func (s *versioningIntegSuite) dispatchContinueAsNew() {
	// TODO: this test will need updating after fixing stickiness, see comments below
	tq := s.randomizeStr(s.T().Name())

	started1 := make(chan struct{})
	started11 := make(chan struct{})
	started2 := make(chan struct{})

	wf1 := func(ctx workflow.Context, attempt int) (string, error) {
		started1 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		// TODO: shouldn't be WithChildOptions
		newCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			VersioningIntent: worker.UseDefaultVersion, // this one should go to latest
		})
		_ = newCtx
		switch attempt {
		case 0:
			// TODO: after fixing stickiness, comment this out:
			return "", workflow.NewContinueAsNewError(ctx, "wf", attempt+1)
		case 1:
			// return "", workflow.NewContinueAsNewError(newCtx, "wf", attempt+1)
		case 2:
			// return "done!", nil
		}
		panic("oops")
	}
	wf11 := func(ctx workflow.Context, attempt int) (string, error) {
		started11 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		newCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			VersioningIntent: worker.UseDefaultVersion,
		})
		_ = newCtx
		switch attempt {
		case 0:
			// TODO: after fixing stickiness, uncomment this:
			// return "", workflow.NewContinueAsNewError(ctx, "wf", attempt+1)
		case 1:
			return "", workflow.NewContinueAsNewError(newCtx, "wf", attempt+1)
		case 2:
			// return "done!", nil
		}
		panic("oops")
	}
	wf2 := func(ctx workflow.Context, attempt int) (string, error) {
		started2 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		newCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			VersioningIntent: worker.UseDefaultVersion,
		})
		_ = newCtx
		switch attempt {
		case 0:
			// return "",workflow.NewContinueAsNewError(ctx, "wf", attempt+1)
		case 1:
			// return "", workflow.NewContinueAsNewError(newCtx, "wf", attempt+1)
		case 2:
			return "done!", nil
		}
		panic("oops")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, "v1")
	s.waitForPropagation(ctx, tq, "v1")

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v1",
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started1)

	// now register v11 as newer compatible with v1 AND v2 as a new default
	s.addCompatibleBuildId(ctx, tq, "v11", "v1", false)
	s.addNewDefaultBuildId(ctx, tq, "v2")
	s.waitForPropagation(ctx, tq, "v2")

	// start workers for v11 and v2
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v11",
		UseBuildIDForVersioning: true,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v2",
		UseBuildIDForVersioning: true,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// unblock the workflow. it should continue on v1 then continue-as-new onto v11
	// TODO: after fixing stickiness, it should continue on v11 and then continue-as-new onto v11.
	// will also need to mess with channels then.
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	// wait for it to start on v11 then unblock. it should continue on v11 then continue-as-new onto v2.
	s.waitForChan(ctx, started11)
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	// wait for it to start on v2 and unblock. it should return.
	s.waitForChan(ctx, started2)
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *versioningIntegSuite) TestDispatchRetry() {
	s.testWithMatchingBehavior(s.dispatchRetry)
}

func (s *versioningIntegSuite) dispatchRetry() {
	// TODO: this test will need updating after fixing stickiness, see comments below
	tq := s.randomizeStr(s.T().Name())

	started1 := make(chan struct{}, 3)
	started11 := make(chan struct{}, 3)

	wf1 := func(ctx workflow.Context) (string, error) {
		started1 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		switch workflow.GetInfo(ctx).Attempt {
		case 1:
			// TODO: stickiness
			return "", errors.New("try again")
		case 2:
			// return "", errors.New("try again")
		case 3:
			// return "done!", nil
		}
		panic("oops")
	}
	wf11 := func(ctx workflow.Context) (string, error) {
		started11 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		switch workflow.GetInfo(ctx).Attempt {
		case 1:
			// TODO: stickiness
			// return "", errors.New("try again")
		case 2:
			return "", errors.New("try again")
		case 3:
			return "done!", nil
		}
		panic("oops")
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		panic("oops")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, "v1")
	s.waitForPropagation(ctx, tq, "v1")

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v1",
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1000 * time.Millisecond,
		},
	}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	<-started1

	// now register v11 as newer compatible with v1 AND v2 as a new default
	s.addCompatibleBuildId(ctx, tq, "v11", "v1", false)
	s.addNewDefaultBuildId(ctx, tq, "v2")
	s.waitForPropagation(ctx, tq, "v2")

	// start workers for v11 and v2
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v11",
		UseBuildIDForVersioning: true,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v2",
		UseBuildIDForVersioning: true,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// unblock the workflow. it should continue on v1 then retry onto v11
	// TODO: after fixing stickiness, it should continue on v11 and then retry onto v11.
	// will also need to mess with channels then.
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	// TODO: fix this hack by making polls interruptable? the problem is w1 still has a poller
	// sitting around blocked since before we changed versioning data
	time.Sleep(500 * time.Millisecond)
	w1.Stop()

	// wait for it to start on v11 then unblock. it should continue on v11 then retry onto v11 again.
	<-started11
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	// wait for it to start on v11 and unblock. it should return.
	<-started11
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *versioningIntegSuite) TestDispatchCron() {
	s.testWithMatchingBehavior(s.dispatchCron)
}

func (s *versioningIntegSuite) dispatchCron() {
	tq := s.randomizeStr(s.T().Name())

	var runs1 atomic.Int32
	var runs11 atomic.Int32
	var runs2 atomic.Int32

	wf1 := func(ctx workflow.Context) (string, error) {
		runs1.Add(1)
		return "ok", nil
	}
	wf11 := func(ctx workflow.Context) (string, error) {
		runs11.Add(1)
		return "ok", nil
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		runs2.Add(1)
		return "ok", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, "v1")
	s.waitForPropagation(ctx, tq, "v1")

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v1",
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	_, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue:                tq,
		CronSchedule:             "@every 1s",
		WorkflowExecutionTimeout: 7 * time.Second,
	}, "wf")
	s.NoError(err)

	// give it ~3 runs on v1
	time.Sleep(3500 * time.Millisecond)

	// now register v11 as newer compatible with v1 AND v2 as a new default.
	// it will run on v2 instead of v11 because cron always starts on default.
	s.addCompatibleBuildId(ctx, tq, "v11", "v1", false)
	s.addNewDefaultBuildId(ctx, tq, "v2")

	// start workers for v11 and v2
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v11",
		UseBuildIDForVersioning: true,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 "v2",
		UseBuildIDForVersioning: true,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// give it ~3 runs on v2
	time.Sleep(3500 * time.Millisecond)

	s.GreaterOrEqual(runs1.Load(), int32(3))
	s.Zero(runs11.Load())
	s.GreaterOrEqual(runs2.Load(), int32(3))
}

// addNewDefaultBuildId updates build id info on a task queue with a new build id in a new default set.
func (s *versioningIntegSuite) addNewDefaultBuildId(ctx context.Context, tq, newBuildId string) {
	res, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: newBuildId,
		},
	})
	s.NoError(err)
	s.NotNil(res)
}

// addCompatibleBuildId updates build id info on a task queue with a new compatible build id.
func (s *versioningIntegSuite) addCompatibleBuildId(ctx context.Context, tq, newBuildId, existing string, makeSetDefault bool) {
	res, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				NewBuildId:                newBuildId,
				ExistingCompatibleBuildId: existing,
				MakeSetDefault:            makeSetDefault,
			},
		},
	})
	s.NoError(err)
	s.NotNil(res)
}

// waitForPropagation waits for all partitions of tq to mention newBuildId in their versioning data (in any position).
func (s *versioningIntegSuite) waitForPropagation(ctx context.Context, tq, newBuildId string) {
	v, ok := s.testCluster.host.dcClient.getRawValue(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	s.True(ok, "versioning tests require setting explicit number of partitions")
	partCount, ok := v.(int)
	s.True(ok, "partition count is not an int")

	type partAndType struct {
		part int
		tp   enumspb.TaskQueueType
	}
	remaining := make(map[partAndType]struct{})
	for i := 0; i < partCount; i++ {
		remaining[partAndType{i, enumspb.TASK_QUEUE_TYPE_ACTIVITY}] = struct{}{}
		remaining[partAndType{i, enumspb.TASK_QUEUE_TYPE_WORKFLOW}] = struct{}{}
	}
	nsId := s.getNamespaceID(s.namespace)
	s.Eventually(func() bool {
		for pt := range remaining {
			partName, err := tqname.FromBaseName(tq)
			s.NoError(err)
			partName = partName.WithPartition(pt.part)
			// Use lower-level GetTaskQueueUserData instead of GetWorkerBuildIdCompatibility
			// here so that we can target activity queues.
			res, err := s.testCluster.host.matchingClient.GetTaskQueueUserData(
				ctx,
				&matchingservice.GetTaskQueueUserDataRequest{
					NamespaceId:   nsId,
					TaskQueue:     partName.FullName(),
					TaskQueueType: pt.tp,
				})
			s.NoError(err)
			if containsBuildId(res.GetUserData().GetData().GetVersioningData(), newBuildId) {
				delete(remaining, pt)
			}
		}
		return len(remaining) == 0
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *versioningIntegSuite) waitForChan(ctx context.Context, ch chan struct{}) {
	s.T().Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		s.FailNow("context timeout")
	}
}

func containsBuildId(data *persistencespb.VersioningData, buildId string) bool {
	for _, set := range data.GetVersionSets() {
		for _, id := range set.BuildIds {
			if id.Id == buildId {
				return true
			}
		}
	}
	return false
}

func getCurrentDefault(res *workflowservice.GetWorkerBuildIdCompatibilityResponse) string {
	if res == nil {
		return ""
	}
	curMajorSet := res.GetMajorVersionSets()[len(res.GetMajorVersionSets())-1]
	return curMajorSet.GetBuildIds()[len(curMajorSet.GetBuildIds())-1]
}
