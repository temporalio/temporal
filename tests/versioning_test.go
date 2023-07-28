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

// nolint:revive
package tests

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
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
	longPollTime        = 5 * time.Second
	// use > 2 pollers by default to expose more timing situations
	numPollers = 4
)

func (s *versioningIntegSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs:     true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs: true,
		dynamicconfig.MatchingForwarderMaxChildrenPerNode:        partitionTreeDegree,
		dynamicconfig.TaskQueuesPerBuildIdLimit:                  3,

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:   1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstPerInstance: 1000,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS:                          1000,

		// The dispatch tests below rely on being able to see the effects of changing
		// versioning data relatively quickly. In general we only promise to act on new
		// versioning data "soon", i.e. after a long poll interval. We can reduce the long poll
		// interval so that we don't have to wait so long.
		dynamicconfig.MatchingLongPollExpirationInterval: longPollTime,
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

	foo := s.prefixed("foo")
	s.addNewDefaultBuildId(ctx, tq, foo)

	res2, err := s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal(foo, getCurrentDefault(res2))
}

func (s *versioningIntegSuite) TestSeriesOfUpdates() {
	ctx := NewContext()
	tq := "integration-versioning-series"

	for i := 0; i < 10; i++ {
		s.addNewDefaultBuildId(ctx, tq, s.prefixed(fmt.Sprintf("foo-%d", i)))
	}
	s.addCompatibleBuildId(ctx, tq, s.prefixed("foo-2.1"), s.prefixed("foo-2"), false)

	res, err := s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res)
	s.Equal(s.prefixed("foo-9"), getCurrentDefault(res))
	s.Equal(s.prefixed("foo-2.1"), res.GetMajorVersionSets()[2].GetBuildIds()[1])
	s.Equal(s.prefixed("foo-2"), res.GetMajorVersionSets()[2].GetBuildIds()[0])
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
	tq := "integration-versioning-persists"

	s.addNewDefaultBuildId(ctx, tq, s.prefixed("foo"))

	// Unload task queue to make sure the data is there when we load it again.
	s.unloadTaskQueue(ctx, tq)

	res, err := s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res)
	s.Equal(s.prefixed("foo"), getCurrentDefault(res))
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

func (s *versioningIntegSuite) TestMaxTaskQueuesPerBuildIdEnforced() {
	ctx := NewContext()
	buildId := fmt.Sprintf("b-%s", s.T().Name())
	// Map a 3 task queues to this build id and verify success
	for i := 1; i <= 3; i++ {
		taskQueue := fmt.Sprintf("q-%s-%d", s.T().Name(), i)
		_, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
			Namespace: s.namespace,
			TaskQueue: taskQueue,
			Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
				AddNewBuildIdInNewDefaultSet: buildId,
			},
		})
		s.NoError(err)
	}

	// Map a fourth task queue to this build id and verify it errors
	taskQueue := fmt.Sprintf("q-%s-%d", s.T().Name(), 4)
	_, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: buildId,
		},
	})
	var failedPreconditionError *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionError)
	s.Equal("Exceeded max task queues allowed to be mapped to a single build id: 3", failedPreconditionError.Message)
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
	v1 := s.prefixed("v1")

	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
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

func (s *versioningIntegSuite) TestDispatchNotUsingVersioning() {
	s.testWithMatchingBehavior(s.dispatchNotUsingVersioning)
}

func (s *versioningIntegSuite) dispatchNotUsingVersioning() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	wf1nover := func(ctx workflow.Context) (string, error) {
		return "done without versioning!", nil
	}
	wf1 := func(ctx workflow.Context) (string, error) {
		return "done with versioning!", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1nover := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          false,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1nover.RegisterWorkflowWithOptions(wf1nover, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1nover.Start())
	defer w1nover.Stop()
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done with versioning!", out)
}

func (s *versioningIntegSuite) TestDispatchNewWorkflowStartWorkerFirst() {
	s.testWithMatchingBehavior(s.dispatchNewWorkflowStartWorkerFirst)
}

func (s *versioningIntegSuite) dispatchNewWorkflowStartWorkerFirst() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	// run worker before registering build. it will use guessed set id
	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	// wait for it to start polling
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *versioningIntegSuite) TestDisableUserData_DefaultTasksBecomeUnversioned() {
	// force one partition so that we can unload the task queue
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueWritePartitions)

	tq := s.randomizeStr(s.T().Name())
	v0 := s.prefixed("v0")

	// Register a versioned "v0" worker to execute a single workflow task to constrain a workflow on the task queue to a
	// compatible set.
	ch := make(chan struct{}, 1)
	wf1 := func(ctx workflow.Context) (string, error) {
		close(ch)
		workflow.GetSignalChannel(ctx, "unblock").Receive(ctx, nil)
		return "done!", nil
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v0,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflow(wf1)
	s.NoError(w1.Start())
	defer w1.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v0)
	s.waitForPropagation(ctx, tq, v0)

	// Start the first workflow while the task queue is still considered versioned.
	// We want to verify that if a spooled task with a "compatible" versioning directive doesn't block a spooled task
	// with a "default" directive.
	// This should never happen in practice since we dispatch "default" tasks to the unversioned task queue but the test
	// verifies this at a functional level.
	run1, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf1)
	s.NoError(err)

	// Wait for first WFT and stop the worker
	<-ch
	w1.Stop()

	// Generate a second workflow task with a "compatible" directive, it should be spooled in the versioned task queue.
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run1.GetID(), run1.GetRunID(), "unblock", nil))

	wf2 := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}
	run2, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf2)
	s.NoError(err)

	// Wait a bit and allow tasks to be spooled.
	time.Sleep(time.Second * 3)

	// Disable user data and unload the task queue.
	dc.OverrideValue(dynamicconfig.MatchingLoadUserData, false)
	defer dc.RemoveOverride(dynamicconfig.MatchingLoadUserData)
	s.unloadTaskQueue(ctx, tq)

	// Start an unversioned worker and verify that the second workflow completes.
	w2 := worker.New(s.sdkClient, tq, worker.Options{
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflow(wf2)
	s.NoError(w2.Start())
	defer w2.Stop()

	var out string
	s.NoError(run2.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *versioningIntegSuite) TestDispatchUnversionedRemainsUnversioned() {
	s.testWithMatchingBehavior(s.dispatchUnversionedRemainsUnversioned)
}

func (s *versioningIntegSuite) dispatchUnversionedRemainsUnversioned() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *versioningIntegSuite) TestDispatchUpgradeStopOld() {
	s.testWithMatchingBehavior(func() { s.dispatchUpgrade(true) })
}

func (s *versioningIntegSuite) TestDispatchUpgradeWait() {
	s.testWithMatchingBehavior(func() { s.dispatchUpgrade(false) })
}

func (s *versioningIntegSuite) dispatchUpgrade(stopOld bool) {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")

	started := make(chan struct{}, 1)

	wf1 := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return "done!", nil
	}

	wf11 := func(ctx workflow.Context) (string, error) {
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return "done from 1.1!", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	s.waitForChan(ctx, started)

	// now add v11 as compatible so the next workflow task runs there
	s.addCompatibleBuildId(ctx, tq, v11, v1, false)
	s.waitForPropagation(ctx, tq, v11)
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond)

	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()

	// Two cases:
	if stopOld {
		// Stop the old worker. Workflow tasks will go to the sticky queue, which will see that
		// it's not the latest and kick them back to the normal queue, which will be dispatched
		// to v11.
		w1.Stop()
	} else {
		// Don't stop the old worker. In this case, w1 will still have some pollers blocked on
		// the normal queue which could pick up tasks that we want to go to v11. (We don't
		// interrupt long polls.) To ensure those polls don't interfere, wait for them to
		// expire.
		time.Sleep(longPollTime)
	}

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done from 1.1!", out)
}

type activityFailMode int

const (
	dontFailActivity = iota
	failActivity
	timeoutActivity
)

func (s *versioningIntegSuite) TestDispatchActivity() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(dontFailActivity) })
}

func (s *versioningIntegSuite) TestDispatchActivityFail() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(failActivity) })
}

func (s *versioningIntegSuite) TestDispatchActivityTimeout() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(timeoutActivity) })
}

func (s *versioningIntegSuite) dispatchActivity(failMode activityFailMode) {
	// This also implicitly tests that a workflow stays on a compatible version set if a new
	// incompatible set is registered, because wf2 just panics. It further tests that
	// stickiness on v1 is not broken by registering v2, because the channel send will panic on
	// replay after we close the channel.

	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	started := make(chan struct{}, 1)

	var act1state, act2state atomic.Int32

	act1 := func() (string, error) {
		if act1state.Add(1) == 1 {
			switch failMode {
			case failActivity:
				return "", errors.New("try again")
			case timeoutActivity:
				time.Sleep(5 * time.Second)
				return "ignored", nil
			}
		}
		return "v1", nil
	}
	act2 := func() (string, error) {
		if act2state.Add(1) == 1 {
			switch failMode {
			case failActivity:
				return "", errors.New("try again")
			case timeoutActivity:
				time.Sleep(5 * time.Second)
				return "ignored", nil
			}
		}
		return "v2", nil
	}
	wf1 := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		// wait for signal
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		// run two activities
		fut1 := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			DisableEagerExecution:  true,
			VersioningIntent:       temporal.VersioningIntentCompatible,
			StartToCloseTimeout:    1 * time.Second,
		}), "act")
		fut2 := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			DisableEagerExecution:  true,
			VersioningIntent:       temporal.VersioningIntentDefault, // this one should go to default
			StartToCloseTimeout:    1 * time.Second,
		}), "act")
		var val1, val2 string
		s.NoError(fut1.Get(ctx, &val1))
		s.NoError(fut2.Get(ctx, &val2))
		return val1 + val2, nil
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		panic("workflow should not run on v2")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started)
	close(started) // force panic if replayed

	// now register v2 as default
	s.addNewDefaultBuildId(ctx, tq, v2)
	s.waitForPropagation(ctx, tq, v2)
	// start worker for v2
	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
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

func (s *versioningIntegSuite) TestDispatchActivityCompatible() {
	s.testWithMatchingBehavior(s.dispatchActivityCompatible)
}

func (s *versioningIntegSuite) dispatchActivityCompatible() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")

	started := make(chan struct{}, 2)

	act1 := func() (string, error) { return "v1", nil }
	act11 := func() (string, error) { return "v1.1", nil }
	wf1 := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		// wait for signal
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		// run activity
		fut11 := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			DisableEagerExecution:  true,
			VersioningIntent:       temporal.VersioningIntentCompatible,
		}), "act")
		var val11 string
		s.NoError(fut11.Get(ctx, &val11))
		return val11, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started)

	// now register v1.1 as compatible
	s.addCompatibleBuildId(ctx, tq, v11, v1, false)
	s.waitForPropagation(ctx, tq, v11)
	// start worker for v1.1
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w11.RegisterActivityWithOptions(act11, activity.RegisterOptions{Name: "act"})
	s.NoError(w11.Start())
	defer w11.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime)

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("v1.1", out)
}

func (s *versioningIntegSuite) TestDispatchActivityCrossTQFails() {
	dc := s.testCluster.host.dcClient
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueWritePartitions)
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	tq := s.randomizeStr(s.T().Name())
	crosstq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	act := func() (string, error) { return "v1", nil }
	wf := func(ctx workflow.Context) (string, error) {
		fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 1 * time.Second,
			TaskQueue:           crosstq,
			VersioningIntent:    temporal.VersioningIntentCompatible,
		}), "act")
		var val string
		s.NoError(fut.Get(ctx, &val))
		return val, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.addNewDefaultBuildId(ctx, crosstq, v1)
	s.waitForPropagation(ctx, tq, v1)
	s.waitForPropagation(ctx, crosstq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	w1cross := worker.New(s.sdkClient, crosstq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1cross.RegisterActivityWithOptions(act, activity.RegisterOptions{Name: "act"})
	s.NoError(w1cross.Start())
	defer w1cross.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	// workflow should be terminated by invalid argument
	var out string
	s.Error(run.Get(ctx, &out))
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
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

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
			VersioningIntent: temporal.VersioningIntentDefault, // this one should go to default
		}), "child")
		var val1, val2 string
		s.NoError(fut1.Get(ctx, &val1))
		s.NoError(fut2.Get(ctx, &val2))
		return val1 + val2, nil
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		panic("workflow should not run on v2")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
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
	s.addNewDefaultBuildId(ctx, tq, v2)
	s.waitForPropagation(ctx, tq, v2)
	// start worker for v2
	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
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

func (s *versioningIntegSuite) TestDispatchChildWorkflowUpgrade() {
	s.testWithMatchingBehavior(s.dispatchChildWorkflowUpgrade)
}

func (s *versioningIntegSuite) dispatchChildWorkflowUpgrade() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")

	started := make(chan struct{}, 2)

	child1 := func(workflow.Context) (string, error) { return "v1", nil }
	child11 := func(workflow.Context) (string, error) { return "v1.1", nil }
	wf1 := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		// wait for signal
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		// run child
		fut11 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{}), "child")
		var val11 string
		s.NoError(fut11.Get(ctx, &val11))
		return val11, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterWorkflowWithOptions(child1, workflow.RegisterOptions{Name: "child"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started)

	// now register v1.1 as compatible
	s.addCompatibleBuildId(ctx, tq, v11, v1, false)
	s.waitForPropagation(ctx, tq, v11)
	// start worker for v1.1
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w11.RegisterWorkflowWithOptions(child11, workflow.RegisterOptions{Name: "child"})
	s.NoError(w11.Start())
	defer w11.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime)

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("v1.1", out)
}

func (s *versioningIntegSuite) TestDispatchChildWorkflowCrossTQFails() {
	dc := s.testCluster.host.dcClient
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueWritePartitions)
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

	tq := s.randomizeStr(s.T().Name())
	crosstq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	child := func(ctx workflow.Context) (string, error) { return "v1", nil }
	wf := func(ctx workflow.Context) (string, error) {
		fut := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:        crosstq,
			VersioningIntent: temporal.VersioningIntentCompatible,
		}), "child")
		var val string
		s.NoError(fut.Get(ctx, &val))
		return val, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.addNewDefaultBuildId(ctx, crosstq, v1)
	s.waitForPropagation(ctx, tq, v1)
	s.waitForPropagation(ctx, crosstq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	w1cross := worker.New(s.sdkClient, crosstq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1cross.RegisterWorkflowWithOptions(child, workflow.RegisterOptions{Name: "child"})
	s.NoError(w1cross.Start())
	defer w1cross.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	// workflow should be terminated by invalid argument
	var out string
	s.Error(run.Get(ctx, &out))
}

func (s *versioningIntegSuite) TestDispatchQuery() {
	s.testWithMatchingBehavior(s.dispatchQuery)
}

func (s *versioningIntegSuite) dispatchQuery() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	v2 := s.prefixed("v2")

	started := make(chan struct{}, 2)

	wf1 := func(ctx workflow.Context) error {
		if err := workflow.SetQueryHandler(ctx, "query", func() (string, error) { return "v1", nil }); err != nil {
			return err
		}
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return nil
	}
	wf11 := func(ctx workflow.Context) error {
		if err := workflow.SetQueryHandler(ctx, "query", func() (string, error) { return "v1.1", nil }); err != nil {
			return err
		}
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return nil
	}
	wf2 := func(ctx workflow.Context) error {
		if err := workflow.SetQueryHandler(ctx, "query", func() (string, error) { return "v2", nil }); err != nil {
			return err
		}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started)

	// now register v1.1 as compatible
	// now register v11 as newer compatible with v1 AND v2 as a new default
	s.addCompatibleBuildId(ctx, tq, v11, v1, false)
	s.addNewDefaultBuildId(ctx, tq, v2)
	s.waitForPropagation(ctx, tq, v2)
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond)

	// start worker for v1.1 and v2
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()
	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime)

	// query
	val, err := s.sdkClient.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "query")
	s.NoError(err)
	var out string
	s.NoError(val.Get(&out))
	s.Equal("v1.1", out)

	// let the workflow exit
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))
}

func (s *versioningIntegSuite) TestDispatchContinueAsNew() {
	s.testWithMatchingBehavior(s.dispatchContinueAsNew)
}

func (s *versioningIntegSuite) dispatchContinueAsNew() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	v2 := s.prefixed("v2")

	started1 := make(chan struct{}, 10)
	started11 := make(chan struct{}, 20)

	wf1 := func(ctx workflow.Context, attempt int) (string, error) {
		started1 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		switch attempt {
		case 0:
			// return "", workflow.NewContinueAsNewError(ctx, "wf", attempt+1)
		case 1:
			// newCtx := workflow.WithWorkflowVersioningIntent(ctx, temporal.VersioningIntentDefault) // this one should go to default
			// return "", workflow.NewContinueAsNewError(newCtx, "wf", attempt+1)
		case 2:
			// return "done!", nil
		}
		panic("oops")
	}
	wf11 := func(ctx workflow.Context, attempt int) (string, error) {
		started11 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		switch attempt {
		case 0:
			return "", workflow.NewContinueAsNewError(ctx, "wf", attempt+1)
		case 1:
			newCtx := workflow.WithWorkflowVersioningIntent(ctx, temporal.VersioningIntentDefault) // this one should go to default
			return "", workflow.NewContinueAsNewError(newCtx, "wf", attempt+1)
		case 2:
			// return "done!", nil
		}
		panic("oops")
	}
	wf2 := func(ctx workflow.Context, attempt int) (string, error) {
		switch attempt {
		case 2:
			return "done!", nil
		}
		panic("oops")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started1)

	// now register v11 as newer compatible with v1 AND v2 as a new default
	s.addCompatibleBuildId(ctx, tq, v11, v1, false)
	s.addNewDefaultBuildId(ctx, tq, v2)
	s.waitForPropagation(ctx, tq, v2)
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond)

	// start workers for v11 and v2
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime)

	// unblock the workflow. it should get kicked off the sticky queue and replay on v11
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))
	s.waitForChan(ctx, started11)

	// then continue-as-new onto v11
	s.waitForChan(ctx, started11)

	// unblock the second run. it should continue on v11 then continue-as-new onto v2, then
	// complete.
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *versioningIntegSuite) TestDispatchRetry() {
	s.testWithMatchingBehavior(s.dispatchRetry)
}

func (s *versioningIntegSuite) dispatchRetry() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	v2 := s.prefixed("v2")

	started1 := make(chan struct{}, 10)
	started11 := make(chan struct{}, 30)

	wf1 := func(ctx workflow.Context) (string, error) {
		started1 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		switch workflow.GetInfo(ctx).Attempt {
		case 1:
			// return "", errors.New("try again")
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
			return "", errors.New("try again")
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
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
	s.waitForChan(ctx, started1)

	// now register v11 as newer compatible with v1 AND v2 as a new default
	s.addCompatibleBuildId(ctx, tq, v11, v1, false)
	s.addNewDefaultBuildId(ctx, tq, v2)
	s.waitForPropagation(ctx, tq, v2)
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond)

	// start workers for v11 and v2
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime)

	// unblock the workflow. it should replay on v11 and then retry (on v11).
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))
	s.waitForChan(ctx, started11) // replay
	s.waitForChan(ctx, started11) // attempt 2

	// now it's blocked in attempt 2. unblock it.
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	// wait for attempt 3. unblock that and it should return.
	s.waitForChan(ctx, started11) // attempt 3
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
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	v2 := s.prefixed("v2")

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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
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
	s.addCompatibleBuildId(ctx, tq, v11, v1, false)
	s.addNewDefaultBuildId(ctx, tq, v2)

	// start workers for v11 and v2
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
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

func (s *versioningIntegSuite) TestDisableUserData() {
	tq := s.T().Name()
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First insert some data (we'll try to read it below)
	s.addNewDefaultBuildId(ctx, tq, v1)

	dc := s.testCluster.host.dcClient
	defer dc.RemoveOverride(dynamicconfig.MatchingLoadUserData)
	dc.OverrideValue(dynamicconfig.MatchingLoadUserData, false)

	// unload so that we reload and pick up LoadUserData dynamic config
	s.unloadTaskQueue(ctx, tq)

	// Verify update fails
	_, err := s.engine.UpdateWorkerBuildIdCompatibility(ctx, &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: v2,
		},
	})
	var failedPreconditionError *serviceerror.FailedPrecondition
	s.Require().ErrorAs(err, &failedPreconditionError)

	s.unloadTaskQueue(ctx, tq)

	// Verify read returns empty
	_, err = s.engine.GetWorkerBuildIdCompatibility(ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.Require().ErrorAs(err, &failedPreconditionError)
}

func (s *versioningIntegSuite) TestDisableUserData_UnversionedWorkflowRuns() {
	tq := s.T().Name()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dc := s.testCluster.host.dcClient
	defer dc.RemoveOverride(dynamicconfig.MatchingLoadUserData)
	dc.OverrideValue(dynamicconfig.MatchingLoadUserData, false)

	wf := func(ctx workflow.Context) (string, error) {
		return "ok", nil
	}
	wrk := worker.New(s.sdkClient, tq, worker.Options{})
	wrk.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	s.NoError(wrk.Start())
	defer wrk.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue:                tq,
		WorkflowExecutionTimeout: 5 * time.Second,
	}, "wf")
	s.NoError(err)
	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("ok", out)
}

func (s *versioningIntegSuite) TestDisableUserData_WorkflowGetsStuck() {
	// force one partition so that we can unload the task queue
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueWritePartitions)

	tq := s.T().Name()
	v1 := s.prefixed("v1")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s.addNewDefaultBuildId(ctx, tq, v1)

	dc.OverrideValue(dynamicconfig.MatchingLoadUserData, false)
	defer dc.RemoveOverride(dynamicconfig.MatchingLoadUserData)

	s.unloadTaskQueue(ctx, tq)

	var runs atomic.Int32
	wf := func(ctx workflow.Context) error {
		runs.Add(1)
		return nil
	}
	wrk := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	wrk.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	s.NoError(wrk.Start())
	defer wrk.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue:                tq,
		WorkflowExecutionTimeout: 10 * time.Second,
	}, "wf")
	s.Require().NoError(err)

	// should not run on versioned worker
	time.Sleep(2 * time.Second)
	s.Require().Equal(int32(0), runs.Load())

	wrk.Stop()

	// start unversioned worker and let task run there
	wrk2 := worker.New(s.sdkClient, tq, worker.Options{
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	wrk2.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	s.NoError(wrk2.Start())
	defer wrk2.Stop()

	// now workflow can complete
	err = run.Get(ctx, nil)
	s.NoError(err)
	s.Require().Equal(int32(1), runs.Load())
}

func (s *versioningIntegSuite) TestDisableUserData_QueryFails() {
	// force one partition so that we can unload the task queue
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueWritePartitions)

	tq := s.T().Name()
	v1 := s.prefixed("v1")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)

	var runs atomic.Int32
	wf := func(ctx workflow.Context) error {
		workflow.SetQueryHandler(ctx, "query", func() (string, error) {
			runs.Add(1)
			return "response", nil
		})
		return nil
	}
	wrk := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	wrk.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	s.NoError(wrk.Start())
	defer wrk.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue:                tq,
		WorkflowExecutionTimeout: 5 * time.Second,
	}, "wf")
	s.Require().NoError(err)

	// wait for it to complete
	s.NoError(run.Get(ctx, nil))

	dc.OverrideValue(dynamicconfig.MatchingLoadUserData, false)
	defer dc.RemoveOverride(dynamicconfig.MatchingLoadUserData)

	s.unloadTaskQueue(ctx, tq)

	_, err = s.sdkClient.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "query")
	var failedPrecond *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecond, err)
	s.Equal(int32(0), runs.Load())
}

func (s *versioningIntegSuite) TestDescribeTaskQueue() {
	// force one partition since DescribeTaskQueue only goes to the root
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueWritePartitions)

	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	v2 := s.prefixed("v2")

	wf := func(ctx workflow.Context) (string, error) { return "ok", nil }

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.addCompatibleBuildId(ctx, tq, v11, v1, false)
	s.addNewDefaultBuildId(ctx, tq, v2)
	s.waitForPropagation(ctx, tq, v2)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 v1,
		UseBuildIDForVersioning: true,
		Identity:                s.randomizeStr("id"),
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 v11,
		UseBuildIDForVersioning: true,
		Identity:                s.randomizeStr("id"),
	})
	w11.RegisterWorkflow(wf)
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 v2,
		UseBuildIDForVersioning: true,
		Identity:                s.randomizeStr("id"),
	})
	w2.RegisterWorkflow(wf)
	s.NoError(w2.Start())
	defer w2.Stop()

	s.Eventually(func() bool {
		resp, err := s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:     s.namespace,
			TaskQueue:     &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		})
		s.NoError(err)
		havePoller := func(v string) bool {
			for _, p := range resp.Pollers {
				if p.WorkerVersionCapabilities.UseVersioning && v == p.WorkerVersionCapabilities.BuildId {
					return true
				}
			}
			return false
		}
		// v1 polls get rejected because v11 is newer
		return !havePoller(v1) && havePoller(v11) && havePoller(v2)
	}, 3*time.Second, 50*time.Millisecond)
}

func (s *versioningIntegSuite) TestDescribeWorkflowExecution() {
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	dc.OverrideValue(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueReadPartitions)
	defer dc.RemoveOverride(dynamicconfig.MatchingNumTaskqueueWritePartitions)

	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")

	started1 := make(chan struct{}, 10)
	started11 := make(chan struct{}, 10)

	wf := func(ctx workflow.Context) (string, error) {
		started1 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		started11 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return "ok", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForPropagation(ctx, tq, v1)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 v1,
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started1)

	// describe and check build id
	s.Eventually(func() bool {
		resp, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), "")
		s.NoError(err)
		return v1 == resp.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId()
	}, 5*time.Second, 100*time.Millisecond)

	// now register v11 as newer compatible with v1
	s.addCompatibleBuildId(ctx, tq, v11, v1, false)
	s.waitForPropagation(ctx, tq, v11)
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond)

	// start worker for v11
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                 v11,
		UseBuildIDForVersioning: true,
	})
	w11.RegisterWorkflow(wf)
	s.NoError(w11.Start())
	defer w11.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime)

	// unblock the workflow. it should get kicked off the sticky queue and replay on v11
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))
	s.waitForChan(ctx, started11)

	s.Eventually(func() bool {
		resp, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), "")
		s.NoError(err)
		return v11 == resp.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId()
	}, 5*time.Second, 100*time.Millisecond)

	// unblock. it should complete
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))
	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("ok", out)
}

// Add a per test prefix to avoid hitting the namespace limit of mapped task queue per build id
func (s *versioningIntegSuite) prefixed(buildId string) string {
	return fmt.Sprintf("t%x:%s", 0xffff&farm.Hash32([]byte(s.T().Name())), buildId)
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

func (s *versioningIntegSuite) unloadTaskQueue(ctx context.Context, tq string) {
	_, err := s.testCluster.GetMatchingClient().ForceUnloadTaskQueue(ctx, &matchingservice.ForceUnloadTaskQueueRequest{
		NamespaceId:   s.getNamespaceID(s.namespace),
		TaskQueue:     tq,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	s.Require().NoError(err)
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
