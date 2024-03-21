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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/durationpb"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
)

type VersioningIntegSuite struct {
	// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
	// not merely log an error
	*require.Assertions
	FunctionalTestBase
	sdkClient sdkclient.Client
}

const (
	partitionTreeDegree = 3
	longPollTime        = 5 * time.Second
	// use > 2 pollers by default to expose more timing situations
	numPollers = 4
)

func (s *VersioningIntegSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs:     true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs: true,
		dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs:     true,
		dynamicconfig.MatchingForwarderMaxChildrenPerNode:        partitionTreeDegree,
		dynamicconfig.TaskQueuesPerBuildIdLimit:                  3,

		dynamicconfig.AssignmentRuleLimitPerQueue:      10,
		dynamicconfig.RedirectRuleLimitPerQueue:        10,
		dynamicconfig.MatchingDeletedRuleRetentionTime: 24 * time.Hour,

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance:   1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstPerInstance: 1000,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS:                          1000,

		// The dispatch tests below rely on being able to see the effects of changing
		// versioning data relatively quickly. In general, we only promise to act on new
		// versioning data "soon", i.e. after a long poll interval. We can reduce the long poll
		// interval so that we don't have to wait so long.
		dynamicconfig.MatchingLongPollExpirationInterval: longPollTime,
	}
	s.setupSuite("testdata/cluster.yaml")
}

func (s *VersioningIntegSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *VersioningIntegSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	s.sdkClient = sdkClient
}

func (s *VersioningIntegSuite) TearDownTest() {
	s.sdkClient.Close()
}

func (s *VersioningIntegSuite) TestVersionRuleConflictToken() {
	ctx := NewContext()
	tq := "test-conflict-token"

	// nil token --> fail
	s.insertAssignmentRule(ctx, tq, "1", 0, nil, false)

	// correct token from List --> success
	cT1 := s.listVersioningRules(ctx, tq).GetConflictToken()
	cT2 := s.insertAssignmentRule(ctx, tq, "2", 0, cT1, true)

	// confirm token changed on insert but not on list
	cT3 := s.listVersioningRules(ctx, tq).GetConflictToken()
	s.NotEqual(cT1, cT2)
	s.Equal(cT2, cT3)

	// correct token from List or most recent mutation --> success
	cT4 := s.insertAssignmentRule(ctx, tq, "3", 0, cT2, true)
	s.listVersioningRules(ctx, tq)

	// wrong token fails, same request with nil token also fails
	s.insertAssignmentRule(ctx, tq, "4", 0, cT1, false)
	s.insertAssignmentRule(ctx, tq, "4", 0, nil, false)

	// wrong token fails, same request with correct token from Update succeeds
	s.replaceAssignmentRule(ctx, tq, "20", 0, cT2, false)
	cT5 := s.replaceAssignmentRule(ctx, tq, "20", 0, cT4, true)
	cT6 := s.listVersioningRules(ctx, tq).GetConflictToken()

	// confirm that list didn't change the conflict token again
	s.Equal(cT5, cT6)

	// wrong token fails, same request with correct token from List succeeds
	s.deleteAssignmentRule(ctx, tq, 0, cT4, false)
	s.deleteAssignmentRule(ctx, tq, 0, cT6, true)
  
	// nil token fails
	s.deleteAssignmentRule(ctx, tq, 0, nil, false)
}

func (s *VersioningIntegSuite) TestAssignmentRuleInsert() {
	// setup
	ctx := NewContext()
	tq := "test-assignment-rule-insert"

	// get initial conflict token
	cT := s.listVersioningRules(ctx, tq).GetConflictToken()

	// success
	cT = s.insertAssignmentRule(ctx, tq, "1", 0, cT, true)
	res1 := s.listVersioningRules(ctx, tq)
	s.Equal("1", res1.GetAssignmentRules()[0].GetRule().GetTargetBuildId())

	// failure due to out of bounds index
	s.insertAssignmentRule(ctx, tq, "2", -1, cT, false)
	s.Equal(res1, s.listVersioningRules(ctx, tq))

	// success with conflict token returned by last successful call, same as above
	s.insertAssignmentRule(ctx, tq, "2", 1, cT, true)
	s.Equal("2", s.listVersioningRules(ctx, tq).GetAssignmentRules()[1].GetRule().GetTargetBuildId())
}

func (s *VersioningIntegSuite) TestAssignmentRuleReplace() {
	// setup
	ctx := NewContext()
	tq := "test-assignment-rule-replace"

	// get initial conflict token + do initial inserts
	cT := s.listVersioningRules(ctx, tq).GetConflictToken()
	cT = s.insertAssignmentRule(ctx, tq, "1", 0, cT, true)
	cT = s.insertAssignmentRule(ctx, tq, "2", 0, cT, true)

	// success
	cT = s.replaceAssignmentRule(ctx, tq, "3", 0, cT, true)
	res := s.listVersioningRules(ctx, tq)
	s.Equal("3", res.GetAssignmentRules()[0].GetRule().GetTargetBuildId())

	// failure due to index out of bounds
	s.replaceAssignmentRule(ctx, tq, "4", 10, cT, false)
	s.Equal(res, s.listVersioningRules(ctx, tq))

	// success with conflict token returned by last successful call, same as above
	s.replaceAssignmentRule(ctx, tq, "4", 0, cT, true)
	s.Equal("4", s.listVersioningRules(ctx, tq).GetAssignmentRules()[0].GetRule().GetTargetBuildId())
}

func (s *VersioningIntegSuite) TestAssignmentRuleDelete() {
	// setup
	ctx := NewContext()
	tq := "test-assignment-rule-delete"

	// get initial conflict token + do initial inserts
	cT := s.listVersioningRules(ctx, tq).GetConflictToken()
	cT = s.insertAssignmentRule(ctx, tq, "1", 0, cT, true)
	cT = s.insertAssignmentRule(ctx, tq, "2", 0, cT, true)

	// success
	cT = s.deleteAssignmentRule(ctx, tq, 0, cT, true)
	res := s.listVersioningRules(ctx, tq)
	s.Equal(1, len(res.GetAssignmentRules()))

	// failure due to requirement that once an unconditional rule exists, at least one must always exist
	s.deleteAssignmentRule(ctx, tq, 0, cT, false)
	s.Equal(res, s.listVersioningRules(ctx, tq))

	// insert another rule to prove that the conflict token was not the issue above
	cT = s.insertAssignmentRule(ctx, tq, "2", 0, cT, true)

	// delete again, success
	s.deleteAssignmentRule(ctx, tq, 0, cT, true)
	s.Equal(1, len(res.GetAssignmentRules()))

}

func (s *VersioningIntegSuite) TestRedirectRuleInsert() {
	// setup
	ctx := NewContext()
	tq := "test-redirect-rule-insert"

	// get initial conflict token
	cT := s.listVersioningRules(ctx, tq).GetConflictToken()

	// success
	cT = s.insertRedirectRule(ctx, tq, "1", "0", cT, true)
	res := s.listVersioningRules(ctx, tq)
	rulesMap := mkRedirectRulesMap(res.GetCompatibleRedirectRules())
	s.Contains(rulesMap, "1")
	s.Equal(rulesMap["1"], "0")

	// failure due to cycle
	s.insertRedirectRule(ctx, tq, "0", "1", cT, false)
	s.Equal(res, s.listVersioningRules(ctx, tq))

	// success with same conflict token but no cycle
	s.insertRedirectRule(ctx, tq, "0", "2", cT, true)
}

func (s *VersioningIntegSuite) TestRedirectRuleReplace() {
	// setup
	ctx := NewContext()
	tq := "test-redirect-rule-replace"

	// get initial conflict token + do initial insert
	cT := s.listVersioningRules(ctx, tq).GetConflictToken()
	cT = s.insertRedirectRule(ctx, tq, "1", "0", cT, true)

	// success
	cT = s.replaceRedirectRule(ctx, tq, "1", "2", cT, true)
	res := s.listVersioningRules(ctx, tq)
	rulesMap := mkRedirectRulesMap(res.GetCompatibleRedirectRules())
	s.Contains(rulesMap, "1")
	s.Equal(rulesMap["1"], "2")

	// failure due to source not found
	s.replaceRedirectRule(ctx, tq, "10", "3", cT, false)
	s.Equal(res, s.listVersioningRules(ctx, tq))

	// success with same conflict token and correct source
	s.replaceRedirectRule(ctx, tq, "1", "3", cT, true)
}

func (s *VersioningIntegSuite) TestRedirectRuleDelete() {
	// setup
	ctx := NewContext()
	tq := "test-redirect-rule-delete"

	// get initial conflict token + do initial inserts
	cT := s.listVersioningRules(ctx, tq).GetConflictToken()
	cT = s.insertRedirectRule(ctx, tq, "1", "0", cT, true)
	cT = s.insertRedirectRule(ctx, tq, "2", "0", cT, true)

	// success
	cT = s.deleteRedirectRule(ctx, tq, "1", cT, true)
	res := s.listVersioningRules(ctx, tq)
	s.Equal(1, len(res.GetCompatibleRedirectRules()))

	// failure due to source not found
	s.deleteRedirectRule(ctx, tq, "1", cT, false)
	s.Equal(res, s.listVersioningRules(ctx, tq))

	// success with same conflict token and valid source
	s.deleteRedirectRule(ctx, tq, "2", cT, true)
}

func (s *VersioningIntegSuite) TestCommitBuildID() {
	// setup
	ctx := NewContext()
	tq := "test-commit-build-id"

	// no recent poller --> failure
	s.commitBuildId(ctx, tq, "1", false, nil, false)

	// no recent poller + force --> success
	s.commitBuildId(ctx, tq, "1", true, nil, true)
	res := s.listVersioningRules(ctx, tq)
	s.Equal(1, len(res.GetAssignmentRules()))
	s.Equal(0, len(res.GetCompatibleRedirectRules()))
	s.Equal("1", res.GetAssignmentRules()[0].GetRule().GetTargetBuildId())
	s.Equal(nil, res.GetAssignmentRules()[0].GetRule().GetRamp())

	// recent versioned poller on wrong build id --> failure
	s.pollVersionedTaskQueue(tq, "3", true)
	s.commitBuildId(ctx, tq, "2", false, nil, false)

	// recent unversioned poller on build id 2 --> failure
	s.pollVersionedTaskQueue(tq, "2", false)
	s.commitBuildId(ctx, tq, "2", false, nil, false)

	// recent versioned poller on build id 2 --> success
	s.pollVersionedTaskQueue(tq, "2", true)
	s.commitBuildId(ctx, tq, "2", false, nil, true)
	res = s.listVersioningRules(ctx, tq)
	s.Equal(1, len(res.GetAssignmentRules()))
	s.Equal(0, len(res.GetCompatibleRedirectRules()))
	s.Equal("2", res.GetAssignmentRules()[0].GetRule().GetTargetBuildId())
	s.Equal(nil, res.GetAssignmentRules()[0].GetRule().GetRamp())

	// todo: test that the recentness of the poller goes away. Not sure how to do this without putting the 70s time window in the dynamic config so that we can make it shorter for the test.
}

func mkRedirectRulesMap(redirectRules []*taskqueuepb.TimestampedCompatibleBuildIdRedirectRule) map[string]string {
	ret := make(map[string]string)
	for _, r := range redirectRules {
		rule := r.GetRule()
		ret[rule.GetSourceBuildId()] = rule.GetTargetBuildId()
	}
	return ret
}

func (s *VersioningIntegSuite) TestBasicVersionUpdate() {
	ctx := NewContext()
	tq := "functional-versioning-basic"

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

func (s *VersioningIntegSuite) TestSeriesOfUpdates() {
	ctx := NewContext()
	tq := "functional-versioning-series"

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

func (s *VersioningIntegSuite) TestLinkToNonexistentCompatibleVersionReturnsNotFound() {
	ctx := NewContext()
	tq := "functional-versioning-compat-not-found"

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

func (s *VersioningIntegSuite) TestVersioningStatePersistsAcrossUnload() {
	ctx := NewContext()
	tq := "functional-versioning-persists"

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

func (s *VersioningIntegSuite) TestVersioningChangesPropagate() {
	ctx := NewContext()
	tq := "functional-versioning-propagate"

	// ensure at least two hops
	const partCount = 1 + partitionTreeDegree + partitionTreeDegree*partitionTreeDegree

	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, partCount)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, partCount)

	for _, buildId := range []string{"foo", "foo-v2", "foo-v3"} {
		s.addNewDefaultBuildId(ctx, tq, buildId)
		s.waitForVersionSetPropagation(ctx, tq, buildId)
	}
}

func (s *VersioningIntegSuite) TestMaxTaskQueuesPerBuildIdEnforced() {
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

func (s *VersioningIntegSuite) testWithMatchingBehavior(subtest func()) {
	dc := s.testCluster.host.dcClient
	for _, forceForward := range []bool{false, true} {
		for _, forceAsync := range []bool{false, true} {
			name := "NoForward"
			if forceForward {
				// force two levels of forwarding
				name = "ForceForward"
			}
			if forceAsync {
				name += "ForceAsync"
			} else {
				name += "AllowSync"
			}

			s.Run(name, func() {
				if forceForward {
					dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, 13)
					dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, 13)
					dc.OverrideValue(s.T(), dynamicconfig.TestMatchingLBForceReadPartition, 5)
					dc.OverrideValue(s.T(), dynamicconfig.TestMatchingLBForceWritePartition, 11)
				} else {
					dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
					dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
				}
				if forceAsync {
					dc.OverrideValue(s.T(), dynamicconfig.TestMatchingDisableSyncMatch, true)
				} else {
					dc.OverrideValue(s.T(), dynamicconfig.TestMatchingDisableSyncMatch, false)
				}

				subtest()
			})
		}
	}
}

func (s *VersioningIntegSuite) TestDispatchNewWorkflow() {
	s.testWithMatchingBehavior(s.dispatchNewWorkflow)
}

func (s *VersioningIntegSuite) dispatchNewWorkflow() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForVersionSetPropagation(ctx, tq, v1)

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

func (s *VersioningIntegSuite) TestDispatchNewWorkflowV2() {
	s.testWithMatchingBehavior(s.dispatchNewWorkflowV2)
}

func (s *VersioningIntegSuite) dispatchNewWorkflowV2() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

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

func (s *VersioningIntegSuite) TestDispatchNotUsingVersioning() {
	s.testWithMatchingBehavior(s.dispatchNotUsingVersioning)
}

func (s *VersioningIntegSuite) dispatchNotUsingVersioning() {
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
	s.waitForVersionSetPropagation(ctx, tq, v1)

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

func (s *VersioningIntegSuite) TestDispatchNewWorkflowStartWorkerFirst() {
	s.testWithMatchingBehavior(s.dispatchNewWorkflowStartWorkerFirst)
}

func (s *VersioningIntegSuite) dispatchNewWorkflowStartWorkerFirst() {
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
	s.waitForVersionSetPropagation(ctx, tq, v1)

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *VersioningIntegSuite) TestDispatchUnversionedRemainsUnversioned() {
	s.testWithMatchingBehavior(s.dispatchUnversionedRemainsUnversioned)
}

func (s *VersioningIntegSuite) dispatchUnversionedRemainsUnversioned() {
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
	s.waitForVersionSetPropagation(ctx, tq, v1)

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
}

func (s *VersioningIntegSuite) TestDispatchUpgradeStopOld() {
	s.testWithMatchingBehavior(func() { s.dispatchUpgrade(true) })
}

func (s *VersioningIntegSuite) TestDispatchUpgradeWait() {
	s.testWithMatchingBehavior(func() { s.dispatchUpgrade(false) })
}

func (s *VersioningIntegSuite) dispatchUpgrade(stopOld bool) {
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
	s.waitForVersionSetPropagation(ctx, tq, v1)

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
	s.waitForVersionSetPropagation(ctx, tq, v11)
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

func (s *VersioningIntegSuite) TestDispatchActivity() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(dontFailActivity) })
}

func (s *VersioningIntegSuite) TestDispatchActivityFail() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(failActivity) })
}

func (s *VersioningIntegSuite) TestDispatchActivityTimeout() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(timeoutActivity) })
}

func (s *VersioningIntegSuite) dispatchActivity(failMode activityFailMode) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForVersionSetPropagation(ctx, tq, v1)

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
	s.waitForVersionSetPropagation(ctx, tq, v2)
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

func (s *VersioningIntegSuite) TestDispatchActivityCompatible() {
	s.testWithMatchingBehavior(s.dispatchActivityCompatible)
}

func (s *VersioningIntegSuite) dispatchActivityCompatible() {
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
	s.waitForVersionSetPropagation(ctx, tq, v1)

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
	s.waitForVersionSetPropagation(ctx, tq, v11)
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

func (s *VersioningIntegSuite) TestDispatchActivityEager() {
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.EnableActivityEagerExecution, true)

	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.Require().NoError(err)

	pollResponse, err := s.sdkClient.WorkflowService().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq},
		Identity:  "test",
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			BuildId: v1,
		},
	})
	s.Require().NoError(err)
	startToCloseTimeout := time.Minute

	completionResponse, err := s.sdkClient.WorkflowService().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity: "test",
		WorkerVersionStamp: &commonpb.WorkerVersionStamp{
			BuildId:       v1,
			UseVersioning: true,
		},
		TaskToken: pollResponse.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId: "compatible",
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: tq,
						},
						StartToCloseTimeout: durationpb.New(startToCloseTimeout),
						ActivityType: &commonpb.ActivityType{
							Name: "ignore",
						},
						RequestEagerExecution: true,
						UseWorkflowBuildId:    true,
					},
				},
			},
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId: "latest",
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: tq,
						},
						StartToCloseTimeout: durationpb.New(startToCloseTimeout),
						ActivityType: &commonpb.ActivityType{
							Name: "ignore",
						},
						RequestEagerExecution: true,
						UseWorkflowBuildId:    false,
					},
				},
			},
		},
	})
	s.Require().NoError(err)
	s.Require().Equal(1, len(completionResponse.ActivityTasks))
	s.Require().Equal("compatible", completionResponse.ActivityTasks[0].ActivityId)
}

func (s *VersioningIntegSuite) TestDispatchActivityCrossTQFails() {
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

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
	s.waitForVersionSetPropagation(ctx, tq, v1)
	s.waitForVersionSetPropagation(ctx, crosstq, v1)

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

func (s *VersioningIntegSuite) TestDispatchChildWorkflow() {
	s.testWithMatchingBehavior(s.dispatchChildWorkflow)
}

func (s *VersioningIntegSuite) dispatchChildWorkflow() {
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
	s.waitForVersionSetPropagation(ctx, tq, v1)

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
	s.waitForVersionSetPropagation(ctx, tq, v2)
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

func (s *VersioningIntegSuite) TestDispatchChildWorkflowUpgrade() {
	s.testWithMatchingBehavior(s.dispatchChildWorkflowUpgrade)
}

func (s *VersioningIntegSuite) dispatchChildWorkflowUpgrade() {
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
	s.waitForVersionSetPropagation(ctx, tq, v1)

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
	s.waitForVersionSetPropagation(ctx, tq, v11)
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

func (s *VersioningIntegSuite) TestDispatchChildWorkflowCrossTQFails() {
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

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
	s.waitForVersionSetPropagation(ctx, tq, v1)
	s.waitForVersionSetPropagation(ctx, crosstq, v1)

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

func (s *VersioningIntegSuite) TestDispatchQuery() {
	s.testWithMatchingBehavior(s.dispatchQuery)
}

func (s *VersioningIntegSuite) dispatchQuery() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	v2 := s.prefixed("v2")

	started := make(chan struct{}, 10)

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
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.addNewDefaultBuildId(ctx, tq, v1)
	s.waitForVersionSetPropagation(ctx, tq, v1)

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
	s.waitForVersionSetPropagation(ctx, tq, v2)
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

	// let the workflow complete
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	// wait for completion
	s.NoError(run.Get(ctx, nil))

	// query on closed workflow
	val, err = s.sdkClient.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), "query")
	s.NoError(err)
	s.NoError(val.Get(&out))
	s.Equal("v1.1", out)

	// start another wf on v2. should complete immediately.
	run2, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	// wait for completion
	s.NoError(run2.Get(ctx, nil))

	// query on closed workflow
	val, err = s.sdkClient.QueryWorkflow(ctx, run2.GetID(), run2.GetRunID(), "query")
	s.NoError(err)
	s.NoError(val.Get(&out))
	s.Equal("v2", out)
}

func (s *VersioningIntegSuite) TestDispatchContinueAsNew() {
	s.testWithMatchingBehavior(s.dispatchContinueAsNew)
}

func (s *VersioningIntegSuite) dispatchContinueAsNew() {
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
	s.waitForVersionSetPropagation(ctx, tq, v1)

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
	s.waitForVersionSetPropagation(ctx, tq, v2)
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

func (s *VersioningIntegSuite) TestDispatchRetry() {
	s.testWithMatchingBehavior(s.dispatchRetry)
}

func (s *VersioningIntegSuite) dispatchRetry() {
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
	s.waitForVersionSetPropagation(ctx, tq, v1)

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
	s.waitForVersionSetPropagation(ctx, tq, v2)
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

func (s *VersioningIntegSuite) TestDispatchCron() {
	s.testWithMatchingBehavior(s.dispatchCron)
}

func (s *VersioningIntegSuite) dispatchCron() {
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
	s.waitForVersionSetPropagation(ctx, tq, v1)

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

func (s *VersioningIntegSuite) TestDescribeTaskQueue() {
	// force one partition since DescribeTaskQueue only goes to the root
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)

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
	s.waitForVersionSetPropagation(ctx, tq, v2)

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

func (s *VersioningIntegSuite) TestDescribeWorkflowExecution() {
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueReadPartitions, 4)
	dc.OverrideValue(s.T(), dynamicconfig.MatchingNumTaskqueueWritePartitions, 4)

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
	s.waitForVersionSetPropagation(ctx, tq, v1)

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
	s.waitForVersionSetPropagation(ctx, tq, v11)
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
func (s *VersioningIntegSuite) prefixed(buildId string) string {
	return fmt.Sprintf("t%x:%s", 0xffff&farm.Hash32([]byte(s.T().Name())), buildId)
}

// listVersioningRules lists rules and checks that the result is successful, returning the response.
func (s *VersioningIntegSuite) listVersioningRules(
	ctx context.Context, tq string) *workflowservice.ListWorkerVersioningRulesResponse {
	res, err := s.engine.ListWorkerVersioningRules(ctx, &workflowservice.ListWorkerVersioningRulesRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res)
	return res
}

// insertAssignmentRule replaces a new versioning assignment rule to the task queue user data.
// It checks the response and returns the conflict token.
func (s *VersioningIntegSuite) insertAssignmentRule(
	ctx context.Context, tq, newBuildId string,
	idx int32, conflictToken []byte, expectSuccess bool) []byte {
	res, err := s.engine.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     s.namespace,
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertAssignmentRule{
			InsertAssignmentRule: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule{
				RuleIndex: idx,
				Rule: &taskqueuepb.BuildIdAssignmentRule{
					TargetBuildId: newBuildId,
				},
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		return res.GetConflictToken()
	} else {
		s.Error(err)
		s.Nil(res)
		return nil
	}
}

// replaceAssignmentRule replaces a new versioning assignment rule to the task queue user data.
// It checks the response and returns the conflict token.
func (s *VersioningIntegSuite) replaceAssignmentRule(
	ctx context.Context, tq, newBuildId string,
	idx int32, conflictToken []byte, expectSuccess bool) []byte {
	res, err := s.engine.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     s.namespace,
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceAssignmentRule{
			ReplaceAssignmentRule: &workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule{
				RuleIndex: idx,
				Rule: &taskqueuepb.BuildIdAssignmentRule{
					TargetBuildId: newBuildId,
				},
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		return res.GetConflictToken()
	} else {
		s.Error(err)
		s.Nil(res)
		return nil
	}
}

// deleteAssignmentRule deletes the versioning assignment rule at a given index.
// It checks the response and returns the conflict token.
func (s *VersioningIntegSuite) deleteAssignmentRule(
	ctx context.Context, tq string,
	idx int32, conflictToken []byte, expectSuccess bool) []byte {
	res, err := s.engine.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     s.namespace,
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_DeleteAssignmentRule{
			DeleteAssignmentRule: &workflowservice.UpdateWorkerVersioningRulesRequest_DeleteBuildIdAssignmentRule{
				RuleIndex: idx,
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		return res.GetConflictToken()
	} else {
		s.Error(err)
		s.Nil(res)
		return nil
	}
}

// insertRedirectRule replaces a new versioning redirect rule to the task queue user data.
// It checks the response and returns the conflict token.
func (s *VersioningIntegSuite) insertRedirectRule(
	ctx context.Context, tq, sourceBuildId, targetBuildId string,
	conflictToken []byte, expectSuccess bool) []byte {
	res, err := s.engine.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     s.namespace,
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertCompatibleRedirectRule{
			InsertCompatibleRedirectRule: &workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule{
				Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{
					SourceBuildId: sourceBuildId,
					TargetBuildId: targetBuildId,
				},
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		return res.GetConflictToken()
	} else {
		s.Error(err)
		s.Nil(res)
		return nil
	}
}

// replaceRedirectRule replaces a new versioning redirect rule to the task queue user data.
// It checks the response and returns the conflict token.
func (s *VersioningIntegSuite) replaceRedirectRule(
	ctx context.Context, tq, sourceBuildId, targetBuildId string,
	conflictToken []byte, expectSuccess bool) []byte {
	res, err := s.engine.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     s.namespace,
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleRedirectRule{
			ReplaceCompatibleRedirectRule: &workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule{
				Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{
					SourceBuildId: sourceBuildId,
					TargetBuildId: targetBuildId,
				},
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		return res.GetConflictToken()
	} else {
		s.Error(err)
		s.Nil(res)
		return nil
	}
}

// deleteRedirectRule deletes the versioning redirect rule at a given index.
// It checks the response and returns the conflict token.
func (s *VersioningIntegSuite) deleteRedirectRule(
	ctx context.Context, tq, sourceBuildId string,
	conflictToken []byte, expectSuccess bool) []byte {
	res, err := s.engine.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     s.namespace,
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleRedirectRule{
			DeleteCompatibleRedirectRule: &workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule{
				SourceBuildId: sourceBuildId,
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		return res.GetConflictToken()
	} else {
		s.Error(err)
		s.Nil(res)
		return nil
	}
}

// commitBuildId sends a CommitBuildId request for the given build ID
// It checks the response and returns the conflict token.
func (s *VersioningIntegSuite) commitBuildId(
	ctx context.Context, tq, targetBuildId string, force bool,
	conflictToken []byte, expectSuccess bool) []byte {
	res, err := s.engine.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     s.namespace,
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_CommitBuildId_{
			CommitBuildId: &workflowservice.UpdateWorkerVersioningRulesRequest_CommitBuildId{
				TargetBuildId: targetBuildId,
				Force:         force,
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		return res.GetConflictToken()
	} else {
		s.Error(err)
		s.Nil(res)
		return nil
	}
}

func (s *VersioningIntegSuite) pollVersionedTaskQueue(tq, buildID string, useVersioning bool) {
	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          buildID,
		UseBuildIDForVersioning:          useVersioning,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	// wait for it to start polling
	time.Sleep(200 * time.Millisecond)
}

// addNewDefaultBuildId updates build id info on a task queue with a new build id in a new default set.
func (s *VersioningIntegSuite) addNewDefaultBuildId(ctx context.Context, tq, newBuildId string) {
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

func (s *VersioningIntegSuite) addAssignmentRule(ctx context.Context, tq, buildId string) *taskqueuepb.BuildIdAssignmentRule {
	cT := s.listVersioningRules(ctx, tq).GetConflictToken()
	rule := &taskqueuepb.BuildIdAssignmentRule{
		TargetBuildId: buildId,
	}
	res, err := s.engine.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     s.namespace,
		TaskQueue:     tq,
		ConflictToken: cT,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertAssignmentRule{
			InsertAssignmentRule: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule{
				Rule: rule,
			},
		},
	})
	s.NoError(err)
	s.NotNil(res)
	return rule
}

// addCompatibleBuildId updates build id info on a task queue with a new compatible build id.
func (s *VersioningIntegSuite) addCompatibleBuildId(ctx context.Context, tq, newBuildId, existing string, makeSetDefault bool) {
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

// waitForVersionSetPropagation waits for all partitions of tq to mention newBuildId in their versioning data (in any position).
func (s *VersioningIntegSuite) waitForVersionSetPropagation(ctx context.Context, taskQueue, newBuildId string) {
	s.waitForPropagation(ctx, taskQueue, func(vd *persistencespb.VersioningData) bool {
		for _, set := range vd.GetVersionSets() {
			for _, id := range set.BuildIds {
				if id.Id == newBuildId {
					return true
				}
			}
		}
		return false
	})
}

// waitForAssignmentRulePropagation waits for all partitions of tq to have the given assignment rule in their versioning data
func (s *VersioningIntegSuite) waitForAssignmentRulePropagation(ctx context.Context, taskQueue string, rule *taskqueuepb.BuildIdAssignmentRule) {
	s.waitForPropagation(ctx, taskQueue, func(vd *persistencespb.VersioningData) bool {
		for _, r := range vd.GetAssignmentRules() {
			if r.GetRule().Equal(rule) {
				return true
			}
		}
		return false
	})
}

// waitForPropagation waits for all partitions of tq to mention newBuildId in their versioning data (in any position).
func (s *VersioningIntegSuite) waitForPropagation(
	ctx context.Context,
	taskQueue string,
	condition func(data *persistencespb.VersioningData) bool,
) {
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
			f, err := tqid.NewTaskQueueFamily(nsId, taskQueue)
			s.NoError(err)
			partition := f.TaskQueue(pt.tp).NormalPartition(pt.part)
			// Use lower-level GetTaskQueueUserData instead of GetWorkerBuildIdCompatibility
			// here so that we can target activity queues.
			res, err := s.testCluster.host.matchingClient.GetTaskQueueUserData(
				ctx,
				&matchingservice.GetTaskQueueUserDataRequest{
					NamespaceId:   nsId,
					TaskQueue:     partition.RpcName(),
					TaskQueueType: partition.TaskType(),
				})
			s.NoError(err)
			if condition(res.GetUserData().GetData().GetVersioningData()) {
				delete(remaining, pt)
			}
		}
		return len(remaining) == 0
	}, 10*time.Second, 100*time.Millisecond)
}

func (s *VersioningIntegSuite) waitForChan(ctx context.Context, ch chan struct{}) {
	s.T().Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		s.FailNow("context timeout")
	}
}

func (s *VersioningIntegSuite) unloadTaskQueue(ctx context.Context, tq string) {
	_, err := s.testCluster.GetMatchingClient().ForceUnloadTaskQueue(ctx, &matchingservice.ForceUnloadTaskQueueRequest{
		NamespaceId:   s.getNamespaceID(s.namespace),
		TaskQueue:     tq,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	s.Require().NoError(err)
}

func (s *VersioningIntegSuite) getStickyQueueName(ctx context.Context, id string) string {
	ms, err := s.adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: id},
	})
	s.NoError(err)
	return ms.DatabaseMutableState.ExecutionInfo.StickyTaskQueue
}

func getCurrentDefault(res *workflowservice.GetWorkerBuildIdCompatibilityResponse) string {
	if res == nil {
		return ""
	}
	curMajorSet := res.GetMajorVersionSets()[len(res.GetMajorVersionSets())-1]
	return curMajorSet.GetBuildIds()[len(curMajorSet.GetBuildIds())-1]
}
