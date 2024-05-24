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
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
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
	// These TTLs need to be greater than the time it takes for a workflow execution status change to show up in visibility
	testReachabilityCacheOpenWFsTTL   = 3 * time.Millisecond
	testReachabilityCacheClosedWFsTTL = 6 * time.Millisecond
)

func (s *VersioningIntegSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendEnableWorkerVersioningDataAPIs.Key():     true,
		dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs.Key(): true,
		dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs.Key():     true,
		dynamicconfig.MatchingForwarderMaxChildrenPerNode.Key():        partitionTreeDegree,
		dynamicconfig.TaskQueuesPerBuildIdLimit.Key():                  3,

		dynamicconfig.AssignmentRuleLimitPerQueue.Key():              10,
		dynamicconfig.RedirectRuleLimitPerQueue.Key():                10,
		dynamicconfig.RedirectRuleMaxUpstreamBuildIDsPerQueue.Key():  10,
		dynamicconfig.MatchingDeletedRuleRetentionTime.Key():         24 * time.Hour,
		dynamicconfig.ReachabilityBuildIdVisibilityGracePeriod.Key(): 3 * time.Minute,
		dynamicconfig.ReachabilityQueryBuildIdLimit.Key():            4,
		dynamicconfig.ReachabilityCacheOpenWFsTTL.Key():              testReachabilityCacheOpenWFsTTL,
		dynamicconfig.ReachabilityCacheClosedWFsTTL.Key():            testReachabilityCacheClosedWFsTTL,

		// Make sure we don't hit the rate limiter in tests
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key():                1000,
		dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance.Key(): 1,
		dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS.Key():                               1000,

		// The dispatch tests below rely on being able to see the effects of changing
		// versioning data relatively quickly. In general, we only promise to act on new
		// versioning data "soon", i.e. after a long poll interval. We can reduce the long poll
		// interval so that we don't have to wait so long.
		// TODO: after cleaning up old versioning we may not need to depend on long poll time. [cleanup-old-wv]
		// This is because we don't interrupt long polls on change of versioning
		// data, so if polls from different build ids go to the same matcher, old ones can steal
		// tasks from new ones. If polls from different build ids never go to the same matcher
		// anymore then we don't need it.
		dynamicconfig.MatchingLongPollExpirationInterval.Key(): longPollTime,

		// this is overridden for tests using testWithMatchingBehavior
		dynamicconfig.MatchingNumTaskqueueReadPartitions.Key():  4,
		dynamicconfig.MatchingNumTaskqueueWritePartitions.Key(): 4,
	}
	s.setupSuite("testdata/es_cluster.yaml")
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
	cT1 := s.getVersioningRules(ctx, tq).GetConflictToken()
	cT2 := s.insertAssignmentRule(ctx, tq, "2", 0, cT1, true)

	// confirm token changed on insert but not on list
	cT3 := s.getVersioningRules(ctx, tq).GetConflictToken()
	s.NotEqual(cT1, cT2)
	s.Equal(cT2, cT3)

	// correct token from List or most recent mutation --> success
	cT4 := s.insertAssignmentRule(ctx, tq, "3", 0, cT2, true)
	s.getVersioningRules(ctx, tq)

	// wrong token fails, same request with nil token also fails
	s.insertAssignmentRule(ctx, tq, "4", 0, cT1, false)
	s.insertAssignmentRule(ctx, tq, "4", 0, nil, false)

	// wrong token fails, same request with correct token from Update succeeds
	s.replaceAssignmentRule(ctx, tq, "20", 0, cT2, false)
	cT5 := s.replaceAssignmentRule(ctx, tq, "20", 0, cT4, true)
	cT6 := s.getVersioningRules(ctx, tq).GetConflictToken()

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
	cT := s.getVersioningRules(ctx, tq).GetConflictToken()

	// success
	cT = s.insertAssignmentRule(ctx, tq, "1", 0, cT, true)
	res1 := s.getVersioningRules(ctx, tq)
	s.Equal("1", res1.GetAssignmentRules()[0].GetRule().GetTargetBuildId())

	// failure due to out of bounds index
	s.insertAssignmentRule(ctx, tq, "2", -1, cT, false)
	s.Equal(res1, s.getVersioningRules(ctx, tq))

	// success with conflict token returned by last successful call, same as above
	s.insertAssignmentRule(ctx, tq, "2", 1, cT, true)
	s.Equal("2", s.getVersioningRules(ctx, tq).GetAssignmentRules()[1].GetRule().GetTargetBuildId())
}

func (s *VersioningIntegSuite) TestAssignmentRuleReplace() {
	// setup
	ctx := NewContext()
	tq := "test-assignment-rule-replace"

	// get initial conflict token + do initial inserts
	cT := s.getVersioningRules(ctx, tq).GetConflictToken()
	cT = s.insertAssignmentRule(ctx, tq, "1", 0, cT, true)
	cT = s.insertAssignmentRule(ctx, tq, "2", 0, cT, true)

	// success
	cT = s.replaceAssignmentRule(ctx, tq, "3", 0, cT, true)
	res := s.getVersioningRules(ctx, tq)
	s.Equal("3", res.GetAssignmentRules()[0].GetRule().GetTargetBuildId())

	// failure due to index out of bounds
	s.replaceAssignmentRule(ctx, tq, "4", 10, cT, false)
	s.Equal(res, s.getVersioningRules(ctx, tq))

	// success with conflict token returned by last successful call, same as above
	s.replaceAssignmentRule(ctx, tq, "4", 0, cT, true)
	s.Equal("4", s.getVersioningRules(ctx, tq).GetAssignmentRules()[0].GetRule().GetTargetBuildId())
}

func (s *VersioningIntegSuite) TestAssignmentRuleDelete() {
	// setup
	ctx := NewContext()
	tq := "test-assignment-rule-delete"

	// get initial conflict token + do initial inserts
	cT := s.getVersioningRules(ctx, tq).GetConflictToken()
	cT = s.insertAssignmentRule(ctx, tq, "1", 0, cT, true)
	cT = s.insertAssignmentRule(ctx, tq, "2", 0, cT, true)

	// success
	cT = s.deleteAssignmentRule(ctx, tq, 0, cT, true)
	res := s.getVersioningRules(ctx, tq)
	s.Equal(1, len(res.GetAssignmentRules()))

	// failure due to requirement that once an unconditional rule exists, at least one must always exist
	s.deleteAssignmentRule(ctx, tq, 0, cT, false)
	s.Equal(res, s.getVersioningRules(ctx, tq))

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
	cT := s.getVersioningRules(ctx, tq).GetConflictToken()

	// success
	cT = s.insertRedirectRule(ctx, tq, "1", "0", cT, true)
	res := s.getVersioningRules(ctx, tq)
	rulesMap := mkRedirectRulesMap(res.GetCompatibleRedirectRules())
	s.Contains(rulesMap, "1")
	s.Equal(rulesMap["1"], "0")

	// failure due to cycle
	s.insertRedirectRule(ctx, tq, "0", "1", cT, false)
	s.Equal(res, s.getVersioningRules(ctx, tq))

	// success with same conflict token but no cycle
	s.insertRedirectRule(ctx, tq, "0", "2", cT, true)
}

func (s *VersioningIntegSuite) TestRedirectRuleReplace() {
	// setup
	ctx := NewContext()
	tq := "test-redirect-rule-replace"

	// get initial conflict token + do initial insert
	cT := s.getVersioningRules(ctx, tq).GetConflictToken()
	cT = s.insertRedirectRule(ctx, tq, "1", "0", cT, true)

	// success
	cT = s.replaceRedirectRule(ctx, tq, "1", "2", cT, true)
	res := s.getVersioningRules(ctx, tq)
	rulesMap := mkRedirectRulesMap(res.GetCompatibleRedirectRules())
	s.Contains(rulesMap, "1")
	s.Equal(rulesMap["1"], "2")

	// failure due to source not found
	s.replaceRedirectRule(ctx, tq, "10", "3", cT, false)
	s.Equal(res, s.getVersioningRules(ctx, tq))

	// success with same conflict token and correct source
	s.replaceRedirectRule(ctx, tq, "1", "3", cT, true)
}

func (s *VersioningIntegSuite) TestRedirectRuleDelete() {
	// setup
	ctx := NewContext()
	tq := "test-redirect-rule-delete"

	// get initial conflict token + do initial inserts
	cT := s.getVersioningRules(ctx, tq).GetConflictToken()
	cT = s.insertRedirectRule(ctx, tq, "1", "0", cT, true)
	cT = s.insertRedirectRule(ctx, tq, "2", "0", cT, true)

	// success
	cT = s.deleteRedirectRule(ctx, tq, "1", cT, true)
	res := s.getVersioningRules(ctx, tq)
	s.Equal(1, len(res.GetCompatibleRedirectRules()))

	// failure due to source not found
	s.deleteRedirectRule(ctx, tq, "1", cT, false)
	s.Equal(res, s.getVersioningRules(ctx, tq))

	// success with same conflict token and valid source
	s.deleteRedirectRule(ctx, tq, "2", cT, true)
}

func (s *VersioningIntegSuite) TestCommitBuildID() {
	// setup
	ctx := NewContext()
	tq := "test-commit-build-id"

	// get initial conflict token
	cT := s.getVersioningRules(ctx, tq).GetConflictToken()

	// no recent poller --> failure
	s.commitBuildId(ctx, tq, "1", false, cT, false)

	// no recent poller + force --> success
	cT = s.commitBuildId(ctx, tq, "1", true, cT, true)
	res := s.getVersioningRules(ctx, tq)
	s.Equal(1, len(res.GetAssignmentRules()))
	s.Equal(0, len(res.GetCompatibleRedirectRules()))
	s.Equal("1", res.GetAssignmentRules()[0].GetRule().GetTargetBuildId())
	s.Equal(nil, res.GetAssignmentRules()[0].GetRule().GetRamp())

	// recent versioned poller on wrong build ID --> failure
	s.registerWorkflowAndPollVersionedTaskQueue(tq, "3", true)
	s.commitBuildId(ctx, tq, "2", false, cT, false)

	// recent unversioned poller on build ID 2 --> failure
	s.registerWorkflowAndPollVersionedTaskQueue(tq, "2", false)
	s.commitBuildId(ctx, tq, "2", false, cT, false)

	// recent versioned poller on build ID 2 --> success
	s.registerWorkflowAndPollVersionedTaskQueue(tq, "2", true)
	s.commitBuildId(ctx, tq, "2", false, cT, true)
	res = s.getVersioningRules(ctx, tq)
	s.Equal(1, len(res.GetAssignmentRules()))
	s.Equal(0, len(res.GetCompatibleRedirectRules()))
	s.Equal("2", res.GetAssignmentRules()[0].GetRule().GetTargetBuildId())
	s.Equal(nil, res.GetAssignmentRules()[0].GetRule().GetRamp())
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
	// Map a 3 task queues to this build ID and verify success
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

	// Map a fourth task queue to this build ID and verify it errors
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
	s.Equal("Exceeded max task queues allowed to be mapped to a single build ID: 3", failedPreconditionError.Message)
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

func (s *VersioningIntegSuite) TestDispatchNewWorkflowOld() {
	s.testWithMatchingBehavior(func() { s.dispatchNewWorkflow(false) })
}

func (s *VersioningIntegSuite) TestDispatchNewWorkflow() {
	s.testWithMatchingBehavior(func() { s.dispatchNewWorkflow(true) })
}

func (s *VersioningIntegSuite) dispatchNewWorkflow(newVersioning bool) {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v1)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v1)
		s.waitForVersionSetPropagation(ctx, tq, v1)
	}

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

	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, newVersioning, v1, "", nil)
	if newVersioning {
		s.validateWorkflowEventsVersionStamps(ctx, run.GetID(), run.GetRunID(), []string{v1}, "")
	} else {
		s.validateWorkflowEventsVersionStamps(ctx, run.GetID(), run.GetRunID(), []string{}, "")
	}
}

func (s *VersioningIntegSuite) TestDispatchNewWorkflowWithRamp() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	wf1 := func(ctx workflow.Context) (string, error) {
		return "done v1!", nil
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		return "done v2!", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rule := s.addAssignmentRule(ctx, tq, v1)
	rule2 := s.addAssignmentRuleWithRamp(ctx, tq, v2, 50)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)
	s.waitForAssignmentRulePropagation(ctx, tq, rule2)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()
	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	counter := make(map[string]int)
	for i := 0; i < 50; i++ {
		run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
		s.NoError(err)
		var out string
		s.NoError(run.Get(ctx, &out))
		counter[out]++
	}

	// both builds should've got executions
	s.Greater(counter["done v1!"], 0)
	s.Greater(counter["done v2!"], 0)
	s.Equal(50, counter["done v1!"]+counter["done v2!"])
}

func (s *VersioningIntegSuite) TestWorkflowStaysInBuildId() {
	s.testWithMatchingBehavior(s.workflowStaysInBuildId)
}

func (s *VersioningIntegSuite) workflowStaysInBuildId() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	act1Done := make(chan struct{})
	rulesUpdated := make(chan struct{})

	act1 := func() (string, error) {
		close(act1Done)
		return "act1 done!", nil
	}

	act2 := func() (string, error) {
		s.waitForChan(ctx, rulesUpdated)
		return "act2 done!", nil
	}

	wf := func(ctx workflow.Context) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			DisableEagerExecution: true,
			StartToCloseTimeout:   1 * time.Second,
		}), act1).Get(ctx, &ret)
		s.NoError(err)
		s.Equal("act1 done!", ret)

		// assignment rules change in here

		err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			DisableEagerExecution: true,
			StartToCloseTimeout:   1 * time.Second,
		}), act2).Get(ctx, &ret)
		s.NoError(err)
		s.Equal("act2 done!", ret)
		return "done!", nil
	}

	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflow(wf)
	w1.RegisterActivity(act1)
	w1.RegisterActivity(act2)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)

	s.waitForChan(ctx, act1Done)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, true, v1, "", nil)

	// update rules with v2 as the default build
	rule = s.addAssignmentRule(ctx, tq, v2)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	dw, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	s.NoError(err)
	s.Equal(1, len(dw.GetPendingActivities()))
	s.NotNil(dw.GetPendingActivities()[0].GetUseWorkflowBuildId())

	close(rulesUpdated)
	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, true, v1, "", nil)
	s.validateWorkflowEventsVersionStamps(ctx, run.GetID(), run.GetRunID(), []string{v1, v1, v1, v1, v1}, "")
}

func (s *VersioningIntegSuite) TestUnversionedWorkflowStaysUnversioned() {
	s.testWithMatchingBehavior(s.unversionedWorkflowStaysUnversioned)
}

func (s *VersioningIntegSuite) unversionedWorkflowStaysUnversioned() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	act1Done := make(chan struct{})
	rulesUpdated := make(chan struct{})

	act1 := func() (string, error) {
		close(act1Done)
		return "act1 done!", nil
	}

	act2 := func() (string, error) {
		s.waitForChan(ctx, rulesUpdated)
		return "act2 done!", nil
	}

	wf := func(ctx workflow.Context) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			DisableEagerExecution: true,
			StartToCloseTimeout:   1 * time.Second,
		}), act1).Get(ctx, &ret)
		s.NoError(err)
		s.Equal("act1 done!", ret)

		// assignment rules change in here

		err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			DisableEagerExecution: true,
			StartToCloseTimeout:   1 * time.Second,
		}), act2).Get(ctx, &ret)
		s.NoError(err)
		s.Equal("act2 done!", ret)
		return "done!", nil
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflow(wf)
	w1.RegisterActivity(act1)
	w1.RegisterActivity(act2)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)

	s.waitForChan(ctx, act1Done)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), "", true, "binary-checksum", "", nil)

	// update rules with v1 as the default build
	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	dw, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	s.NoError(err)
	s.Equal(1, len(dw.GetPendingActivities()))
	s.Nil(dw.GetPendingActivities()[0].GetAssignedBuildId())
	close(rulesUpdated)

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), "", true, "binary-checksum", "", nil)
	s.validateWorkflowEventsVersionStamps(ctx, run.GetID(), run.GetRunID(), []string{"", "", "", "", ""}, "")
}

func (s *VersioningIntegSuite) TestFirstWorkflowTaskAssignment_Spooled() {
	s.testWithMatchingBehavior(s.firstWorkflowTaskAssignmentSpooled)
}

func (s *VersioningIntegSuite) firstWorkflowTaskAssignmentSpooled() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	v3 := s.prefixed("v3")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	// start wf without worker
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		// setting to a short timeout to verify first wf task timeout assignment
		WorkflowTaskTimeout: 1 * time.Second,
	}, "wf")
	s.NoError(err)

	// MS should have the correct build ID
	s.waitForWorkflowBuildId(ctx, run.GetID(), run.GetRunID(), v1)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, true, "", "", nil)

	// update latest build to v2
	rule = s.addAssignmentRule(ctx, tq, v2)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	failedTask := make(chan struct{})
	wf1 := func(ctx workflow.Context) (string, error) {
		close(failedTask)
		panic("failing WF task intentionally")
	}

	// run worker on v1 so it can fail the scheduled task
	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	s.waitForChan(ctx, failedTask)

	// After scheduling the second time, now MS should be assigned to v2
	s.waitForWorkflowBuildId(ctx, run.GetID(), run.GetRunID(), v2)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v2, true, "", "", []string{v1})

	// update latest build to v3
	rule = s.addAssignmentRule(ctx, tq, v3)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	timedoutTask := make(chan struct{})
	wf2 := func(ctx workflow.Context) (string, error) {
		time.Sleep(1 * time.Second)
		close(timedoutTask)
		time.Sleep(1 * time.Second)
		return "return after long sleep", nil
	}

	// run worker on v2 so it can timeout the scheduled task
	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	s.waitForChan(ctx, timedoutTask)

	// After scheduling the third time, now MS should be assigned to v3
	s.waitForWorkflowBuildId(ctx, run.GetID(), run.GetRunID(), v3)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v3, true, "", "", []string{v1, v2})

	wf3 := func(ctx workflow.Context) (string, error) {
		return "done on v3!", nil
	}

	// run worker on v3 so it can complete the wf
	w3 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v3,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w3.RegisterWorkflowWithOptions(wf3, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w3.Start())
	defer w3.Stop()

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done on v3!", out)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v3, true, v3, "", []string{v1, v2})
	s.validateWorkflowEventsVersionStamps(
		ctx, run.GetID(), run.GetRunID(), []string{
			v1, // failed wf tasks
			v2, // timed out wf tasks
			v3, // succeeded wf task
		}, "",
	)
}

func (s *VersioningIntegSuite) TestFirstWorkflowTaskAssignment_SyncMatch() {
	s.testWithMatchingBehavior(s.firstWorkflowTaskAssignmentSyncMatch)
}

func (s *VersioningIntegSuite) firstWorkflowTaskAssignmentSyncMatch() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	v3 := s.prefixed("v3")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	// v1 fails the task
	failedTask := make(chan struct{})
	wf1 := func(ctx workflow.Context) (string, error) {
		failedTask <- struct{}{}
		panic("failing WF task intentionally")
	}

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
		// setting to a short timeout to verify first wf task timeout assignment
		WorkflowTaskTimeout: 1 * time.Second,
	}, "wf")
	s.NoError(err)

	// wait for two failures to make sure more attempts does not generate more history tasks
	s.waitForChan(ctx, failedTask)
	s.waitForChan(ctx, failedTask)

	// MS should have the correct build ID
	s.waitForWorkflowBuildId(ctx, run.GetID(), run.GetRunID(), v1)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, true, "", "", nil)

	// v2 times out the task
	timedoutTask := make(chan struct{})
	wf2 := func(ctx workflow.Context) (string, error) {
		time.Sleep(1 * time.Second)
		close(timedoutTask)
		time.Sleep(1 * time.Second)
		return "return after long sleep", nil
	}

	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		DeadlockDetectionTimeout:         5 * time.Second,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// update latest build to v2
	rule = s.addAssignmentRule(ctx, tq, v2)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	// wait for multiple timeouts to make sure more attempts do not generate more history events
	s.waitForChan(ctx, timedoutTask)
	s.waitForChan(ctx, timedoutTask)
	s.waitForChan(ctx, timedoutTask)

	// After scheduling the second time, now MS should be assigned to v2
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v2, true, "", "", []string{v1})

	// v3 can process the task
	wf3 := func(ctx workflow.Context) (string, error) {
		return "done on v3!", nil
	}

	// run worker on v3 so it can complete the wf
	w3 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v3,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w3.RegisterWorkflowWithOptions(wf3, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w3.Start())
	defer w3.Stop()

	// update latest build to v3
	rule = s.addAssignmentRule(ctx, tq, v3)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done on v3!", out)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v3, true, v3, "", []string{v1, v2})
	s.validateWorkflowEventsVersionStamps(
		ctx, run.GetID(), run.GetRunID(), []string{
			v1, // failed wf tasks
			v2, // timed out wf tasks
			v3, // succeeded wf task
		}, "",
	)
}

func (s *VersioningIntegSuite) TestIndependentActivityTaskAssignment_Spooled_VersionedWorkflow() {
	s.testWithMatchingBehavior(func() { s.independentActivityTaskAssignmentSpooled(true) })
}

func (s *VersioningIntegSuite) TestIndependentActivityTaskAssignment_Spooled_UnversionedWorkflow() {
	s.testWithMatchingBehavior(func() { s.independentActivityTaskAssignmentSpooled(false) })
}

func (s *VersioningIntegSuite) independentActivityTaskAssignmentSpooled(versionedWf bool) {
	wfTq := s.randomizeStr(s.T().Name())
	actTq := s.randomizeStr(s.T().Name())
	wfV1 := s.prefixed("wfv1")
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	v3 := s.prefixed("v3")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rule := s.addAssignmentRule(ctx, actTq, v1)
	s.waitForAssignmentRulePropagation(ctx, actTq, rule)
	if versionedWf {
		rule = s.addAssignmentRule(ctx, wfTq, wfV1)
		s.waitForAssignmentRulePropagation(ctx, wfTq, rule)
	}

	wf := func(ctx workflow.Context) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue:           actTq,
			StartToCloseTimeout: 1 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    1 * time.Second,
				BackoffCoefficient: 1,
			},
		}), "act").Get(ctx, &ret)
		s.NoError(err)
		s.Equal("done in v3!", ret)
		return "done!", nil
	}

	// start workflow worker
	wfw := worker.New(s.sdkClient, wfTq, worker.Options{
		BuildID:                          wfV1,
		UseBuildIDForVersioning:          versionedWf,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	wfw.RegisterWorkflow(wf)
	s.NoError(wfw.Start())
	defer wfw.Stop()

	// start wf without activity worker
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: wfTq,
		// setting to a short timeout to verify first wf task timeout assignment
		WorkflowTaskTimeout: 1 * time.Second,
	}, wf)
	s.NoError(err)

	// MS should have the correct build ID after finishing the first WFT
	s.Eventually(
		func() bool {
			dw, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
			s.NoError(err)
			if len(dw.GetPendingActivities()) == 0 {
				return false
			}
			if versionedWf {
				s.Equal(wfV1, dw.GetWorkflowExecutionInfo().GetAssignedBuildId())
				s.Equal(wfV1, dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId())
			} else {
				s.Equal("", dw.GetWorkflowExecutionInfo().GetAssignedBuildId())
				s.False(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetUseVersioning())
			}
			return v1 == dw.GetPendingActivities()[0].GetLastIndependentlyAssignedBuildId()
		},
		10*time.Second,
		50*time.Millisecond,
	)

	// update latest build to v2
	rule = s.addAssignmentRule(ctx, actTq, v2)
	s.waitForAssignmentRulePropagation(ctx, actTq, rule)

	failedTask := make(chan struct{})
	act1 := func() (string, error) {
		close(failedTask)
		return "", errors.New("failing activity task intentionally")
	}

	// run v1 activity worker so it can fail the scheduled activity
	w1 := worker.New(s.sdkClient, actTq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	s.waitForChan(ctx, failedTask)

	// After scheduling the second time, now pending activity should be assigned to v2
	s.Eventually(
		func() bool {
			dw, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
			s.NoError(err)
			s.Equal(1, len(dw.GetPendingActivities()))
			return v2 == dw.GetPendingActivities()[0].GetLastIndependentlyAssignedBuildId()
		},
		10*time.Second,
		50*time.Millisecond,
	)

	// update latest build to v3
	rule = s.addAssignmentRule(ctx, actTq, v3)
	s.waitForAssignmentRulePropagation(ctx, actTq, rule)

	timedoutTask := make(chan struct{})
	act2 := func() (string, error) {
		time.Sleep(1 * time.Second)
		close(timedoutTask)
		time.Sleep(1 * time.Second)
		return "return after long sleep", nil
	}

	// run v2 activity worker so it can timeout the scheduled activity
	w2 := worker.New(s.sdkClient, actTq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act"})
	s.NoError(w2.Start())
	defer w2.Stop()

	s.waitForChan(ctx, timedoutTask)

	// After scheduling the third time, now pending activity should be assigned to v3
	s.Eventually(
		func() bool {
			dw, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
			s.NoError(err)
			s.Equal(1, len(dw.GetPendingActivities()))
			return v3 == dw.GetPendingActivities()[0].GetLastIndependentlyAssignedBuildId()
		},
		10*time.Second,
		50*time.Millisecond,
	)

	act3 := func() (string, error) {
		return "done in v3!", nil
	}

	// run v3 worker so it can complete the activity
	w3 := worker.New(s.sdkClient, actTq, worker.Options{
		BuildID:                          v3,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w3.RegisterActivityWithOptions(act3, activity.RegisterOptions{Name: "act"})
	s.NoError(w3.Start())
	defer w3.Stop()

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)

	wfBuild := ""
	if versionedWf {
		wfBuild = wfV1
	}
	s.validateWorkflowEventsVersionStamps(
		ctx, run.GetID(), run.GetRunID(), []string{
			wfBuild,
			v3, // succeeded activity
			wfBuild,
		}, "",
	)
}

func (s *VersioningIntegSuite) TestIndependentActivityTaskAssignment_SyncMatch_VersionedWorkflow() {
	s.testWithMatchingBehavior(func() { s.independentActivityTaskAssignmentSyncMatch(true) })
}

func (s *VersioningIntegSuite) TestIndependentActivityTaskAssignment_SyncMatch_UnversionedWorkflow() {
	s.testWithMatchingBehavior(func() { s.independentActivityTaskAssignmentSyncMatch(false) })
}

func (s *VersioningIntegSuite) independentActivityTaskAssignmentSyncMatch(versionedWf bool) {
	wfTq := s.randomizeStr(s.T().Name())
	actTq := s.randomizeStr(s.T().Name())
	wfV1 := s.prefixed("wfv1")
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	v3 := s.prefixed("v3")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rule := s.addAssignmentRule(ctx, actTq, v1)
	s.waitForAssignmentRulePropagation(ctx, actTq, rule)
	if versionedWf {
		rule := s.addAssignmentRule(ctx, wfTq, wfV1)
		s.waitForAssignmentRulePropagation(ctx, wfTq, rule)
	}

	wf := func(ctx workflow.Context) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue:           actTq,
			StartToCloseTimeout: 1 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    1 * time.Second,
				BackoffCoefficient: 1,
			},
		}), "act").Get(ctx, &ret)
		s.NoError(err)
		s.Equal("done in v3!", ret)
		return "done!", nil
	}

	// start workflow worker
	wfw := worker.New(s.sdkClient, wfTq, worker.Options{
		BuildID:                          wfV1,
		UseBuildIDForVersioning:          versionedWf,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	wfw.RegisterWorkflow(wf)
	s.NoError(wfw.Start())
	defer wfw.Stop()

	// v1 fails the activity
	failedTask := make(chan struct{})
	act1 := func() (string, error) {
		close(failedTask)
		return "", errors.New("failing activity task intentionally")
	}

	w1 := worker.New(s.sdkClient, actTq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: wfTq,
		// setting to a short timeout to verify first wf task timeout assignment
		WorkflowTaskTimeout: 1 * time.Second,
	}, wf)
	s.NoError(err)

	s.waitForChan(ctx, failedTask)
	time.Sleep(1100 * time.Millisecond)

	// MS should have the correct build ID
	dw, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	s.NoError(err)
	if versionedWf {
		s.Equal(wfV1, dw.GetWorkflowExecutionInfo().GetAssignedBuildId())
		s.Equal(wfV1, dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId())
	} else {
		s.False(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetUseVersioning())
		s.Equal("", dw.GetWorkflowExecutionInfo().GetAssignedBuildId())
	}
	s.Equal(1, len(dw.GetPendingActivities()))
	s.Equal(v1, dw.GetPendingActivities()[0].GetLastIndependentlyAssignedBuildId())

	// v2 timesout the activity
	timedoutTask := make(chan struct{})
	act2 := func() (string, error) {
		time.Sleep(1 * time.Second)
		close(timedoutTask)
		time.Sleep(1 * time.Second)
		return "return after long sleep", nil
	}

	w2 := worker.New(s.sdkClient, actTq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// update latest build to v2
	rule = s.addAssignmentRule(ctx, actTq, v2)
	s.waitForAssignmentRulePropagation(ctx, actTq, rule)

	s.waitForChan(ctx, timedoutTask)
	time.Sleep(1100 * time.Millisecond)

	// After scheduling the second time, now pending activity should be assigned to v2
	dw, err = s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	s.NoError(err)
	s.Equal(1, len(dw.GetPendingActivities()))
	s.Equal(v2, dw.GetPendingActivities()[0].GetLastIndependentlyAssignedBuildId())

	// v3 can process the activity
	act3 := func() (string, error) {
		return "done in v3!", nil
	}

	w3 := worker.New(s.sdkClient, actTq, worker.Options{
		BuildID:                          v3,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w3.RegisterActivityWithOptions(act3, activity.RegisterOptions{Name: "act"})
	s.NoError(w3.Start())
	defer w3.Stop()

	// update latest build to v3
	rule = s.addAssignmentRule(ctx, actTq, v3)
	s.waitForAssignmentRulePropagation(ctx, actTq, rule)

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)

	wfBuild := ""
	if versionedWf {
		wfBuild = wfV1
	}
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), wfBuild, true, wfV1, "", nil)
	s.validateWorkflowEventsVersionStamps(
		ctx, run.GetID(), run.GetRunID(), []string{
			wfBuild,
			v3, // succeeded activity
			wfBuild,
		}, "",
	)
}

func (s *VersioningIntegSuite) TestWorkflowTaskRedirectInRetryFirstTask() {
	s.testWithMatchingBehavior(func() { s.testWorkflowTaskRedirectInRetry(true) })
}

func (s *VersioningIntegSuite) TestWorkflowTaskRedirectInRetryNonFirstTask() {
	s.testWithMatchingBehavior(func() { s.testWorkflowTaskRedirectInRetry(false) })
}

func (s *VersioningIntegSuite) testWorkflowTaskRedirectInRetry(firstTask bool) {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v1.1")
	v12 := s.prefixed("v1.2")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	act := func() error {
		return nil
	}

	// v1 fails the task
	failedTask := make(chan struct{})
	wf1 := func(ctx workflow.Context) (string, error) {
		if !firstTask {
			// add an activity to move workflow past first WFT
			var out interface{}
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 1 * time.Second}), act).Get(ctx, &out)
			s.NoError(err)
		}
		failedTask <- struct{}{}
		panic("failing WF task intentionally")
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivity(act)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		// setting to a short timeout to verify first wf task timeout assignment
		WorkflowTaskTimeout: 1 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			BackoffCoefficient: 1.,
			InitialInterval:    time.Second,
		},
	}, "wf")
	s.NoError(err)

	// wait for multiple failures to make sure more attempts does not generate more history tasks
	s.waitForChan(ctx, failedTask)
	s.waitForChan(ctx, failedTask)
	s.waitForChan(ctx, failedTask)

	expectedStampBuildId := ""
	if !firstTask {
		expectedStampBuildId = v1
	}
	// MS should have the correct build ID
	s.waitForWorkflowBuildId(ctx, run.GetID(), run.GetRunID(), v1)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, true, expectedStampBuildId, "", nil)

	// v11 times out the task
	timedoutTask := make(chan struct{})
	wf11 := func(ctx workflow.Context) (string, error) {
		if !firstTask {
			// add an activity to move workflow past first WFT
			var out interface{}
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 1 * time.Second}), act).Get(ctx, &out)
			s.NoError(err)
		}
		time.Sleep(1 * time.Second)
		timedoutTask <- struct{}{}
		time.Sleep(100 * time.Second)
		return "return after long sleep", nil
	}

	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		DeadlockDetectionTimeout:         3 * time.Second,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	w11.RegisterActivity(act)
	s.NoError(w11.Start())
	defer w11.Stop()

	rule2 := s.addRedirectRule(ctx, tq, v1, v11)
	s.waitForRedirectRulePropagation(ctx, tq, rule2)

	// wait for multiple timeouts to make sure more attempts does not generate more history tasks
	s.waitForChan(ctx, timedoutTask)
	s.waitForChan(ctx, timedoutTask)
	s.waitForChan(ctx, timedoutTask)
	// After scheduling the second time, now MS should be assigned to v2
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v11, true, expectedStampBuildId, "", []string{v1})

	// v12 can process the task
	wf12 := func(ctx workflow.Context) (string, error) {
		if !firstTask {
			// add an activity to move workflow past first WFT
			var out interface{}
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 1 * time.Second}), act).Get(ctx, &out)
			s.NoError(err)
		}
		return "done on v1.2!", nil
	}

	// creating a new client because we do not want to share client with the previous the v11 worker as it keeps
	// the workflow locked for a long time. WF cache is shared across all workers of the same client in Go SDK.
	sdkClient2, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
	// run worker on v12 so it can complete the wf
	w12 := worker.New(sdkClient2, tq, worker.Options{
		BuildID:                          v12,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w12.RegisterWorkflowWithOptions(wf12, workflow.RegisterOptions{Name: "wf"})
	w12.RegisterActivity(act)
	s.NoError(w12.Start())
	defer w12.Stop()

	rule2 = s.addRedirectRule(ctx, tq, v11, v12)
	s.waitForRedirectRulePropagation(ctx, tq, rule2)

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done on v1.2!", out)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v12, true, v12, "", []string{v1, v11})
	expectedStamps := []string{
		v1,  // failed wf task
		v11, // timed out wf task show up in history because a redirect happened after them
		v12, // succeeded wf task
	}
	if !firstTask {
		expectedStamps = []string{
			v1,  // first wf task
			v1,  // activity task
			v1,  // failed wf task on sticky queue
			v1,  // failed wf task on normal queue
			v11, // timed out wf task show up in history because they happened on a different build ID
			v12, // succeeded wf task
		}
	}
	s.validateWorkflowEventsVersionStamps(ctx, run.GetID(), run.GetRunID(), expectedStamps, "")
}

func (s *VersioningIntegSuite) TestDispatchNotUsingVersioningOld() {
	s.testWithMatchingBehavior(func() { s.dispatchNotUsingVersioning(false) })
}

func (s *VersioningIntegSuite) TestDispatchNotUsingVersioning() {
	s.testWithMatchingBehavior(func() { s.dispatchNotUsingVersioning(true) })
}

func (s *VersioningIntegSuite) dispatchNotUsingVersioning(newVersioning bool) {
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

	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v1)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v1)
		s.waitForVersionSetPropagation(ctx, tq, v1)
	}

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

	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, newVersioning, v1, "", nil)
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

	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, false, v1, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchUnversionedRemainsUnversioned() {
	s.testWithMatchingBehavior(s.dispatchUnversionedRemainsUnversioned)
}

func (s *VersioningIntegSuite) dispatchUnversionedRemainsUnversioned() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	started := make(chan struct{}, 1)

	wf := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return "done!", nil
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		// no build ID
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

	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), "", false, "binary-checksum", "", nil)
}

func (s *VersioningIntegSuite) TestDispatchUpgradeStopOldOld() {
	s.testWithMatchingBehavior(func() { s.dispatchUpgrade(false, true) })
}

func (s *VersioningIntegSuite) TestDispatchUpgradeWaitOld() {
	s.testWithMatchingBehavior(func() { s.dispatchUpgrade(false, false) })
}

func (s *VersioningIntegSuite) TestDispatchUpgradeStopOld() {
	s.testWithMatchingBehavior(func() { s.dispatchUpgrade(true, true) })
}

func (s *VersioningIntegSuite) TestDispatchUpgradeWait() {
	s.testWithMatchingBehavior(func() { s.dispatchUpgrade(true, false) })
}

func (s *VersioningIntegSuite) dispatchUpgrade(newVersioning, stopOld bool) {
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

	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v1)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v1)
		s.waitForVersionSetPropagation(ctx, tq, v1)
	}

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
	if newVersioning {
		rule := s.addRedirectRule(ctx, tq, v1, v11)
		s.waitForRedirectRulePropagation(ctx, tq, rule)
	} else {
		s.addCompatibleBuildId(ctx, tq, v11, v1, false)
		s.waitForVersionSetPropagation(ctx, tq, v11)
	}
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

	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v11, newVersioning, v11, "", []string{v1})
}

type activityFailMode int

const (
	dontFailActivity = iota
	failActivity
	timeoutActivity
)

func (s *VersioningIntegSuite) TestDispatchActivityOld() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(dontFailActivity, false, false) })
}

func (s *VersioningIntegSuite) TestDispatchActivityFailOld() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(failActivity, false, false) })
}

func (s *VersioningIntegSuite) TestDispatchActivityTimeoutOld() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(timeoutActivity, false, false) })
}

func (s *VersioningIntegSuite) TestDispatchActivity() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(dontFailActivity, true, false) })
}

func (s *VersioningIntegSuite) TestDispatchActivityFail() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(failActivity, true, false) })
}

func (s *VersioningIntegSuite) TestDispatchActivityTimeout() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(timeoutActivity, true, false) })
}

func (s *VersioningIntegSuite) TestDispatchActivityCrossTq() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(dontFailActivity, true, true) })
}

func (s *VersioningIntegSuite) TestDispatchActivityFailCrossTq() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(failActivity, true, true) })
}

func (s *VersioningIntegSuite) TestDispatchActivityTimeoutCrossTq() {
	s.testWithMatchingBehavior(func() { s.dispatchActivity(timeoutActivity, true, true) })
}

func (s *VersioningIntegSuite) dispatchActivity(failMode activityFailMode, newVersioning bool, crossTq bool) {
	// This also implicitly tests that a workflow stays on a compatible version set if a new
	// incompatible set is registered, because wf2 just panics. It further tests that
	// stickiness on v1 is not broken by registering v2, because the channel send will panic on
	// replay after we close the channel.

	tq := s.randomizeStr(s.T().Name())
	actxTq := s.randomizeStr(s.T().Name() + "activity")
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	started := make(chan struct{}, 1)

	var act1state, act2state atomic.Int32

	doAct := func(state *atomic.Int32, output string) (string, error) {
		if state.Add(1) == 1 {
			switch failMode {
			case failActivity:
				return "", errors.New("try again")
			case timeoutActivity:
				time.Sleep(5 * time.Second)
				return "ignored", nil
			}
		}
		return output, nil
	}

	act1 := func() (string, error) {
		return doAct(&act1state, "v1")
	}
	act2 := func() (string, error) {
		return doAct(&act2state, "v2")
	}
	act1xTq := func() (string, error) {
		return doAct(&act1state, "v1xTq")
	}
	act2xTq := func() (string, error) {
		return doAct(&act2state, "v2xTq")
	}
	wf1 := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		// wait for signal
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		actTq := tq
		if crossTq {
			actTq = actxTq
		}
		// run two activities
		fut1 := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			DisableEagerExecution:  true,
			VersioningIntent:       temporal.VersioningIntentCompatible,
			StartToCloseTimeout:    1 * time.Second,
			TaskQueue:              actTq,
		}), "act")
		fut2 := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			DisableEagerExecution:  true,
			VersioningIntent:       temporal.VersioningIntentDefault, // this one should go to default
			StartToCloseTimeout:    1 * time.Second,
			TaskQueue:              actTq,
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

	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v1)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v1)
		s.waitForVersionSetPropagation(ctx, tq, v1)
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()
	w1xTq := worker.New(s.sdkClient, actxTq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1xTq.RegisterActivityWithOptions(act1xTq, activity.RegisterOptions{Name: "act"})
	s.NoError(w1xTq.Start())
	defer w1xTq.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started)
	close(started) // force panic if replayed

	// now register v2 as default
	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v2)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
		rule = s.addAssignmentRule(ctx, actxTq, v2)
		s.waitForAssignmentRulePropagation(ctx, actxTq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v2)
		s.waitForVersionSetPropagation(ctx, tq, v2)
	}

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
	w2xTq := worker.New(s.sdkClient, actxTq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2xTq.RegisterActivityWithOptions(act2xTq, activity.RegisterOptions{Name: "act"})
	s.NoError(w2xTq.Start())
	defer w2xTq.Stop()

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	if crossTq {
		s.Equal("v1xTqv2xTq", out)
	} else {
		s.Equal("v1v2", out)
	}

	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, newVersioning, v1, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchActivityUpgrade() {
	// Testing:
	// - redirecting dependent activity redirects wf
	//   - this also implicitly verifies that sticky queue is cleared after redirecting activity task
	// - redirecting independent activity does not redirect wf

	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v1.1")
	v12 := s.prefixed("v1.2")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	startedWf := make(chan struct{}, 1)
	proceedWf := make(chan struct{}, 1)
	started11 := make(chan struct{}, 1)
	proceed11 := make(chan struct{}, 1)
	started12 := make(chan struct{}, 1)
	proceed12 := make(chan struct{}, 1)

	act1 := func() (string, error) {
		return "v1", nil
	}
	act11 := func() (string, error) {
		started11 <- struct{}{}
		s.waitForChan(ctx, proceed11)
		return "v1.1", nil
	}
	act12 := func() (string, error) {
		started12 <- struct{}{}
		s.waitForChan(ctx, proceed12)
		return "v1.2", nil
	}
	wf := func(ctx workflow.Context) (string, error) {
		startedWf <- struct{}{}
		// Wait for adding v1 -> v1.1 redirect rule. We use a channel instead of WF signal to make sure the rules are
		// added between WFT started and completed and next task seeing the updated rules is an activity, not a WFT.
		<-proceedWf

		var val1, val2 string
		// run two activities
		err1 := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			DisableEagerExecution:  true,
			VersioningIntent:       temporal.VersioningIntentCompatible,
			StartToCloseTimeout:    1 * time.Second,
		}), "act").Get(ctx, &val1)
		s.NoError(err1)

		startedWf <- struct{}{}
		// Wait for adding v1.1 -> v1.2 redirect rule. We use a channel instead of WF signal to make sure the rules are
		// added between WFT started and completed and next task seeing the updated rules is an activity, not a WFT.
		<-proceedWf

		err2 := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ScheduleToCloseTimeout: time.Minute,
			DisableEagerExecution:  true,
			VersioningIntent:       temporal.VersioningIntentDefault,
			StartToCloseTimeout:    1 * time.Second,
		}), "act").Get(ctx, &val2)
		s.NoError(err2)

		return val1 + val2, nil
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		DeadlockDetectionTimeout:         5 * time.Second,
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		DeadlockDetectionTimeout:         5 * time.Second,
	})
	w11.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	w11.RegisterActivityWithOptions(act11, activity.RegisterOptions{Name: "act"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w12 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v12,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		DeadlockDetectionTimeout:         5 * time.Second,
	})
	w12.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	w12.RegisterActivityWithOptions(act12, activity.RegisterOptions{Name: "act"})
	s.NoError(w12.Start())
	defer w12.Stop()

	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	// wait for it to start on v1
	s.waitForChan(ctx, startedWf)
	rule2 := s.addRedirectRule(ctx, tq, v1, v11)
	s.waitForRedirectRulePropagation(ctx, tq, rule2)
	proceedWf <- struct{}{}

	s.waitForChan(ctx, started11)
	// wf assigned build ID should be updated by activity redirect
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v11, true, v1, "", []string{v1})
	// let activity finish
	proceed11 <- struct{}{}

	// wf replays on 1.1 so need to unblock it an extra time
	s.waitForChan(ctx, startedWf)
	proceedWf <- struct{}{}

	s.waitForChan(ctx, startedWf)
	rule2 = s.addRedirectRule(ctx, tq, v11, v12)
	s.waitForRedirectRulePropagation(ctx, tq, rule2)
	proceedWf <- struct{}{}

	s.waitForChan(ctx, started12)
	// wf assigned build ID should not be updated by independent activity redirect
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v11, true, v11, "", []string{v1})
	// let activity finish
	proceed12 <- struct{}{}

	// wf replays on 1.2 so need to unblock it two extra times
	s.waitForChan(ctx, startedWf)
	proceedWf <- struct{}{}
	s.waitForChan(ctx, startedWf)
	proceedWf <- struct{}{}

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("v1.1v1.2", out)

	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v12, true, v12, "", []string{v1, v11})
}

func (s *VersioningIntegSuite) TestRedirectWithConcurrentActivities() {
	// Testing that wf never "goes back" to older build ID in presence of concurrent activities and random failures.
	//
	// SETUP:
	// 1- Run workers with build IDs v1.0 to v1.9.
	// 2- Workflows runs a nested loop of 10 * `activityRuns`. First loop is per build ID, second is to run parallel
	//    activities for each build ID.
	// 3- Activities fail by `activityErrorRate` probability. Otherwise, they return success after a random wait.
	// 4- The first activity seeing a build ID (i.e. being run by that build ID) removes the redirect rule targeting
	//    that build ID to test the workflow behaves correctly in absence of the applied redirect rule.
	// 5- One activity in each batch is responsible to add redirect rule to the next build ID. since, there is random
	//    delay in activities, more activities of the same batch likely start both before and after this activity.
	// 6- Workflow waits for completion of all activities in a batch before going to the next one.
	//
	// VERIFYING:
	// 1- After each activity is completed, in workflow we verify that it is not completed by a build ID newer of that
	//    of the workflow worker.
	// 2- Workflow finishes. I.e. no task is dropped without rescheduling due to build ID mismatch.
	// 3- At the end of the test we verify that the wf and activity started event build IDs are compatible with
	//        workflows output (which contains the build ID of each completed activity)
	// 4- Redirect counter of all started events is valid
	// 5- Redirect rules were applied at least to one activity.
	// 6- At least one activity was retried.
	// 7- Some history events are unordered based on event timestamp (due to parallel activity random delay)

	// Reduce user data long poll time for faster propagation of the versioning data. This is needed because of the
	// exponential minWaitTime logic in userDataManagerImpl that gets triggered because rules change very fast in
	// this test.
	dc := s.testCluster.host.dcClient
	dc.OverrideValue(s.T(), dynamicconfig.MatchingGetUserDataLongPollTimeout, 2*time.Second)

	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1.0")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	versions := []string{v1}
	for v := 1; v <= 9; v++ {
		versions = append(versions, s.prefixed("v1."+strconv.Itoa(v)))
	}

	activityErrorRate := .2   // chance of each activity attempt fail
	activityRuns := int32(10) // run the activity ~10 times on each version

	activityCounter := atomic.Int32{}
	triggerRedirectAtActivityRun := atomic.Int32{}
	triggerRedirectAtActivityRun.Store(1)

	lastRedirectTarget := atomic.Value{}
	lastRedirectTarget.Store(versions[0] + " redirect cleaned")
	var workers []worker.Worker

	act := func(version string, runId int32) (string, error) {
		runs := activityCounter.Add(1)
		s.T().Logf("Starting activity %d on %s at %d\n", runId, version, runs)
		if lastRedirectTarget.CompareAndSwap(version, version+" observed") && version != versions[0] {
			// The last redirect rule is applied and observed by an activity, now delete it to make sure wf keeps using
			// the right build ID after applying the redirect rule, even when the rule is not present anymore.
			index, err := strconv.Atoi(version[len(version)-1:]) // get the last char of version is the index in the versions array
			s.NoError(err)
			s.T().Logf("Removing redirect from %s to %s \n", versions[index-1], version)
			s.removeRedirectRule(ctx, tq, versions[index-1])
			lastRedirectTarget.CompareAndSwap(version+" observed", version+" redirect cleaned")
		}
		if rand.Float64() < activityErrorRate {
			return "", errors.New("intentionally failing activity")
		}
		if triggerRedirectAtActivityRun.Load() == runId {
			// When enough activities are run using the current version, add redirect rule to the next version.
			v := runId / activityRuns
			if int(v+1) < len(versions) {
				// wait for last redirect rule to be cleaned up
				for !lastRedirectTarget.CompareAndSwap(versions[v]+" redirect cleaned", versions[v+1]) {
				}
				s.T().Logf("Adding redirect from %s to %s at %d\n", versions[v], versions[v+1], runs)
				s.addRedirectRule(ctx, tq, versions[v], versions[v+1])
				// Intentionally do not wait for propagation of the rules to partitions. Waiting will linger this
				// activity and allows all the other concurrent activities to finish, leaving only the WFT task to
				// see the redirect rule for the first time.
				triggerRedirectAtActivityRun.CompareAndSwap(runId, runId+activityRuns)
			}
		}

		// Add random sleep to simulate network delay
		//nolint:forbidigo
		time.Sleep(time.Duration(int64(rand.Intn(50)) * int64(time.Millisecond)))
		s.T().Logf("Completing activity %d on %s at %d\n", runId, version, runs)
		return version, nil
	}

	wf := func(wfCtx workflow.Context, wfVersion string) (string, error) {
		var res []string
		// because of rule propagation delay we run for more than 10 cycles to make sure all versions are seen
		for i := 0; i <= 12; i++ {
			var futures []workflow.Future
			for j := 0; j < int(activityRuns); j++ {
				f := workflow.ExecuteActivity(workflow.WithActivityOptions(
					wfCtx, workflow.ActivityOptions{
						DisableEagerExecution: true,
						VersioningIntent:      temporal.VersioningIntentCompatible,
						StartToCloseTimeout:   200 * time.Millisecond,
						RetryPolicy: &temporal.RetryPolicy{
							InitialInterval:    10 * time.Millisecond,
							BackoffCoefficient: 1,
						},
					}), "act", i*int(activityRuns)+j)
				futures = append(futures, f)
			}

			for _, f := range futures {
				var activityVersion string
				err := f.Get(wfCtx, &activityVersion)
				s.NoError(err)
				res = append(res, activityVersion)
				// The output of a newer build ID should never be sent to a wf worker of an older build ID
				s.Assert().GreaterOrEqual(wfVersion, activityVersion)
				// TODO: uncomment this check once workflow.GetInfo(wfCtx).GetCurrentBuildID() returns correct value
				// based on last started task build ID, not last completed task build ID.
				// s.Assert().GreaterOrEqual(workflow.GetInfo(wfCtx).GetCurrentBuildID(), activityVersion)
			}
		}

		return strings.Join(res, " "), nil
	}

	// run all workers
	for i := 0; i <= 9; i++ {
		v := versions[i]
		w := worker.New(s.sdkClient, tq, worker.Options{
			BuildID:                          v,
			UseBuildIDForVersioning:          true,
			MaxConcurrentWorkflowTaskPollers: numPollers,
			MaxConcurrentActivityTaskPollers: 2,
			// Limit the number of concurrent activities so not all scheduled activities are immediately started.
			MaxConcurrentActivityExecutionSize: 2,
		})
		w.RegisterWorkflowWithOptions(
			func(ctx workflow.Context) (string, error) {
				return wf(ctx, v)
			},
			workflow.RegisterOptions{Name: "wf"})
		w.RegisterActivityWithOptions(
			func(runId int32) (string, error) {
				return act(v, runId)
			},
			activity.RegisterOptions{Name: "act"})
		s.NoError(w.Start())
		workers = append(workers, w)
	}

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	// Workflow should finish, otherwise it may mean we dropped some task without rescheduling them in the new build ID
	var out string
	s.NoError(run.Get(ctx, &out))
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), versions[9], true, versions[9], "", versions[:9])

	activityPerVersion := make(map[string]int)
	for _, v := range strings.Split(out, " ") {
		activityPerVersion[v]++
	}

	wh := s.sdkClient.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	redirectAppliedToActivityTask := false
	activityRetried := false
	sawUnorderedEvents := false
	var maxBuildId string
	var maxStartedTimestamp time.Time
	for wh.HasNext() {
		he, err := wh.Next()
		s.Nil(err)
		var taskStartedStamp *commonpb.WorkerVersionStamp
		var taskRedirectCounter int64
		var buildId string
		if activityStarted := he.GetActivityTaskStartedEventAttributes(); activityStarted != nil {
			taskStartedStamp = activityStarted.GetWorkerVersion()
			buildId = taskStartedStamp.GetBuildId()
			if buildId > maxBuildId {
				redirectAppliedToActivityTask = true
			}
			if activityStarted.Attempt > 1 {
				activityRetried = true
			}
			s.True(taskStartedStamp.GetUseVersioning())
			taskRedirectCounter = activityStarted.GetBuildIdRedirectCounter()
			activityPerVersion[buildId]--
		} else if wfStarted := he.GetWorkflowTaskStartedEventAttributes(); wfStarted != nil {
			taskStartedStamp = wfStarted.GetWorkerVersion()
			s.True(taskStartedStamp.GetUseVersioning())
			buildId = taskStartedStamp.GetBuildId()
			taskRedirectCounter = wfStarted.GetBuildIdRedirectCounter()
		}
		if he.EventTime.AsTime().Before(maxStartedTimestamp) {
			sawUnorderedEvents = true
		} else {
			maxStartedTimestamp = he.EventTime.AsTime()
		}
		if buildId > maxBuildId {
			maxBuildId = buildId
		}
		if taskStartedStamp != nil {
			// the last char of version is the index in the versions array which is the expected redirect counter for
			// a task started event
			expectedRedirectCounter, err := strconv.Atoi(buildId[len(buildId)-1:])
			s.NoError(err)
			s.Equal(expectedRedirectCounter, int(taskRedirectCounter))
		}
	}
	for v, c := range activityPerVersion {
		s.Equal(0, c, "activity count mismatch for build ID "+v)
	}
	// Following validations are more to make sure the test stays correct, rather than testing server's functionality
	s.True(activityRetried, "no activity retried")
	s.True(sawUnorderedEvents)
	s.True(redirectAppliedToActivityTask, "no redirect rule applied to an activity task, is this test broken?")

	for _, w := range workers {
		w.Stop()
	}
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

	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v11, false, v11, "", []string{v1})
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

func (s *VersioningIntegSuite) TestDispatchChildWorkflowOld() {
	s.testWithMatchingBehavior(func() { s.dispatchChildWorkflow(false, false) })
}

func (s *VersioningIntegSuite) TestDispatchChildWorkflow() {
	s.testWithMatchingBehavior(func() { s.dispatchChildWorkflow(true, false) })
}

func (s *VersioningIntegSuite) TestDispatchChildWorkflowCrossTq() {
	s.testWithMatchingBehavior(func() { s.dispatchChildWorkflow(true, true) })
}

func (s *VersioningIntegSuite) dispatchChildWorkflow(newVersioning bool, crossTq bool) {
	// This also implicitly tests that a workflow stays on a compatible version set if a new
	// incompatible set is registered, because wf2 just panics. It further tests that
	// stickiness on v1 is not broken by registering v2, because the channel send will panic on
	// replay after we close the channel.

	tq := s.randomizeStr(s.T().Name())
	childxTq := s.randomizeStr(s.T().Name() + "child")
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	inheritedBuildId := ""
	if newVersioning {
		inheritedBuildId = v1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	started := make(chan struct{}, 1)

	validateChildBuild := func(cctx workflow.Context, expectedBuildId, expectedInheritedBuildId string) {
		exec := workflow.GetInfo(cctx).WorkflowExecution
		expectedStampBuildId := ""
		if !newVersioning && expectedBuildId == v1 {
			expectedStampBuildId = expectedBuildId
		}
		s.validateWorkflowBuildIds(
			ctx,
			exec.ID,
			exec.RunID,
			expectedBuildId,
			newVersioning,
			expectedStampBuildId,
			expectedInheritedBuildId,
			nil,
		)
		if newVersioning {
			s.validateWorkflowEventsVersionStamps(ctx, exec.ID, exec.RunID, []string{expectedBuildId}, expectedInheritedBuildId)
		}
	}

	child1 := func(cctx workflow.Context) (string, error) {
		validateChildBuild(cctx, v1, inheritedBuildId)
		return "v1", nil
	}
	child2 := func(cctx workflow.Context) (string, error) {
		validateChildBuild(cctx, v2, "")
		return "v2", nil
	}
	child1xTq := func(cctx workflow.Context) (string, error) {
		validateChildBuild(cctx, v1, inheritedBuildId)
		return "v1xTq", nil
	}
	child2xTq := func(cctx workflow.Context) (string, error) {
		validateChildBuild(cctx, v2, "")
		return "v2xTq", nil
	}
	wf1 := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		// wait for signal
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		childTq := tq
		if crossTq {
			childTq = childxTq
		}
		// run two child workflows
		fut1 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:        childTq,
			VersioningIntent: temporal.VersioningIntentCompatible,
		}), "child")
		fut2 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:        childTq,
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

	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v1)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
		rule = s.addAssignmentRule(ctx, childxTq, v1)
		s.waitForAssignmentRulePropagation(ctx, childxTq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v1)
		s.waitForVersionSetPropagation(ctx, tq, v1)
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterWorkflowWithOptions(child1, workflow.RegisterOptions{Name: "child"})
	s.NoError(w1.Start())
	defer w1.Stop()
	w1xTq := worker.New(s.sdkClient, childxTq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1xTq.RegisterWorkflowWithOptions(child1xTq, workflow.RegisterOptions{Name: "child"})
	s.NoError(w1xTq.Start())
	defer w1xTq.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started)
	close(started) //force panic if replayed

	// now register v2 as default
	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v2)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
		rule = s.addAssignmentRule(ctx, childxTq, v2)
		s.waitForAssignmentRulePropagation(ctx, childxTq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v2)
		s.waitForVersionSetPropagation(ctx, tq, v2)
	}

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
	w2xTq := worker.New(s.sdkClient, childxTq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2xTq.RegisterWorkflowWithOptions(child2xTq, workflow.RegisterOptions{Name: "child"})
	s.NoError(w2xTq.Start())
	defer w2xTq.Stop()

	// unblock the workflow
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	if crossTq {
		s.Equal("v1xTqv2xTq", out)
	} else {
		s.Equal("v1v2", out)
	}

	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, newVersioning, v1, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchChildWorkflowUpgradeOld() {
	s.testWithMatchingBehavior(func() { s.dispatchChildWorkflowUpgrade(false) })
}

func (s *VersioningIntegSuite) TestDispatchChildWorkflowUpgrade() {
	s.testWithMatchingBehavior(func() { s.dispatchChildWorkflowUpgrade(true) })
}

func (s *VersioningIntegSuite) dispatchChildWorkflowUpgrade(newVersioning bool) {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	inheritedBuildId := ""
	if newVersioning {
		inheritedBuildId = v11
	}

	started := make(chan struct{}, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	child1 := func(cctx workflow.Context) (string, error) {
		exec := workflow.GetInfo(cctx).WorkflowExecution
		expectedStampBuildId := v1
		if newVersioning {
			expectedStampBuildId = ""
		}
		s.validateWorkflowBuildIds(ctx, exec.ID, exec.RunID, v1, newVersioning, expectedStampBuildId, inheritedBuildId, nil)
		return "v1", nil
	}
	child11 := func(cctx workflow.Context) (string, error) {
		exec := workflow.GetInfo(cctx).WorkflowExecution
		expectedStampBuildId := v11
		if newVersioning {
			expectedStampBuildId = ""
		}
		s.validateWorkflowBuildIds(ctx, exec.ID, exec.RunID, v11, newVersioning, expectedStampBuildId, inheritedBuildId, nil)
		return "v1.1", nil
	}
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

	wf11 := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		// wait for signal
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		// run child
		fut11 := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{}), "child")
		var val11 string
		s.NoError(fut11.Get(ctx, &val11))
		return val11, nil
	}

	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v1)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v1)
		s.waitForVersionSetPropagation(ctx, tq, v1)
	}

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
	if newVersioning {
		rule := s.addRedirectRule(ctx, tq, v1, v11)
		s.waitForRedirectRulePropagation(ctx, tq, rule)
	} else {
		s.addCompatibleBuildId(ctx, tq, v11, v1, false)
		s.waitForVersionSetPropagation(ctx, tq, v11)
	}
	// start worker for v1.1
	w11 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
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
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v11, newVersioning, v11, "", []string{v1})
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

func (s *VersioningIntegSuite) TestDispatchContinueAsNewOld() {
	s.testWithMatchingBehavior(func() { s.dispatchContinueAsNew(false, false) })
}

func (s *VersioningIntegSuite) TestDispatchContinueAsNew() {
	s.testWithMatchingBehavior(func() { s.dispatchContinueAsNew(true, false) })
}

func (s *VersioningIntegSuite) TestDispatchContinueAsNewCrossTq() {
	s.testWithMatchingBehavior(func() { s.dispatchContinueAsNew(true, true) })
}

func (s *VersioningIntegSuite) dispatchContinueAsNew(newVersioning bool, crossTq bool) {
	tq := s.randomizeStr(s.T().Name())
	canxTq := s.randomizeStr(s.T().Name() + "CaN")
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	inheritedBuildId := ""
	if newVersioning {
		inheritedBuildId = v1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	started1 := make(chan struct{}, 10)

	wf1 := func(wctx workflow.Context, attempt int) (string, error) {
		switch attempt {
		case 0:
			s.Equal(tq, workflow.GetInfo(wctx).TaskQueueName)
			newCtx := workflow.WithWorkflowVersioningIntent(wctx, temporal.VersioningIntentCompatible)
			if crossTq {
				newCtx = workflow.WithWorkflowTaskQueue(newCtx, canxTq)
			}
			started1 <- struct{}{}
			return "", workflow.NewContinueAsNewError(newCtx, "wf", attempt+1)
		case 1:
			exec := workflow.GetInfo(wctx).WorkflowExecution
			expectedStampBuildId := v1
			if newVersioning {
				expectedStampBuildId = ""
			}
			s.validateWorkflowBuildIds(ctx, exec.ID, exec.RunID, v1, newVersioning, expectedStampBuildId, inheritedBuildId, nil)
			if newVersioning {
				s.validateWorkflowEventsVersionStamps(ctx, exec.ID, exec.RunID, []string{v1}, inheritedBuildId)
			}
			workflow.GetSignalChannel(wctx, "wait").Receive(wctx, nil)
			if crossTq {
				s.Equal(canxTq, workflow.GetInfo(wctx).TaskQueueName)
			} else {
				s.Equal(tq, workflow.GetInfo(wctx).TaskQueueName)
			}
			newCtx := workflow.WithWorkflowVersioningIntent(wctx, temporal.VersioningIntentDefault) // this one should go to default
			if crossTq {
				newCtx = workflow.WithWorkflowTaskQueue(newCtx, canxTq)
			}
			started1 <- struct{}{}
			return "", workflow.NewContinueAsNewError(newCtx, "wf", attempt+1)
		case 2:
			// return "done!", nil
		}
		panic("oops")
	}
	wf2 := func(wctx workflow.Context, attempt int) (string, error) {
		if attempt == 2 {
			exec := workflow.GetInfo(wctx).WorkflowExecution
			s.validateWorkflowBuildIds(ctx, exec.ID, exec.RunID, v2, newVersioning, "", "", nil)
			if crossTq {
				s.Equal(canxTq, workflow.GetInfo(wctx).TaskQueueName)
			} else {
				s.Equal(tq, workflow.GetInfo(wctx).TaskQueueName)
			}
			return "done!", nil
		}
		panic("oops")
	}

	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v1)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
		rule = s.addAssignmentRule(ctx, canxTq, v1)
		s.waitForAssignmentRulePropagation(ctx, canxTq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v1)
		s.waitForVersionSetPropagation(ctx, tq, v1)
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()
	w1xTq := worker.New(s.sdkClient, canxTq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1xTq.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1xTq.Start())
	defer w1xTq.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	s.waitForChan(ctx, started1)

	// now make v2 as a new default
	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v2)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
		rule = s.addAssignmentRule(ctx, canxTq, v2)
		s.waitForAssignmentRulePropagation(ctx, canxTq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v2)
		s.waitForVersionSetPropagation(ctx, tq, v2)
	}
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond)

	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, newVersioning, v1, "", nil)

	// start workers for v2
	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()
	w2xTq := worker.New(s.sdkClient, canxTq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2xTq.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2xTq.Start())
	defer w2xTq.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime)

	// unblock the workflow. it should get kicked off the sticky queue and replay on v1
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))
	// wait for it to start on v1
	s.waitForChan(ctx, started1)

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(ctx, run.GetID(), "", v2, newVersioning, v2, "", nil)
	if newVersioning {
		s.validateWorkflowEventsVersionStamps(ctx, run.GetID(), "", []string{v2}, "")
	}
}

func (s *VersioningIntegSuite) TestDispatchContinueAsNewUpgradeOld() {
	s.testWithMatchingBehavior(func() { s.dispatchContinueAsNewUpgrade(false) })
}

func (s *VersioningIntegSuite) TestDispatchContinueAsNewUpgrade() {
	s.testWithMatchingBehavior(func() { s.dispatchContinueAsNewUpgrade(true) })
}

func (s *VersioningIntegSuite) dispatchContinueAsNewUpgrade(newVersioning bool) {
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

	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v1)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v1)
		s.waitForVersionSetPropagation(ctx, tq, v1)
	}

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
	if newVersioning {
		rule := s.addRedirectRule(ctx, tq, v1, v11)
		s.waitForRedirectRulePropagation(ctx, tq, rule)
		rule2 := s.addAssignmentRule(ctx, tq, v2)
		s.waitForAssignmentRulePropagation(ctx, tq, rule2)
	} else {
		s.addCompatibleBuildId(ctx, tq, v11, v1, false)
		s.addNewDefaultBuildId(ctx, tq, v2)
		s.waitForVersionSetPropagation(ctx, tq, v2)
	}
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

	// initial run
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v11, newVersioning, v11, "", []string{v1})

	inheritedBuildId := ""
	if newVersioning {
		inheritedBuildId = v11
	}
	// first CaN
	s.validateWorkflowBuildIds(ctx, run.GetID(), "", v11, newVersioning, v11, inheritedBuildId, nil)

	// unblock the second run. it should continue on v11 then continue-as-new onto v2, then
	// complete.
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	// second CaN
	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(ctx, run.GetID(), "", v2, newVersioning, v2, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchRetryOld() {
	s.testWithMatchingBehavior(s.dispatchRetryOld)
}

func (s *VersioningIntegSuite) dispatchRetryOld() {
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
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v11, false, v11, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchRetry() {
	s.testWithMatchingBehavior(s.dispatchRetry)
}

func (s *VersioningIntegSuite) dispatchRetry() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	started1 := make(chan struct{}, 10)
	started2 := make(chan struct{}, 10)

	wf1 := func(ctx workflow.Context) (string, error) {
		started1 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		if workflow.GetInfo(ctx).Attempt == 1 {
			return "", errors.New("try again")
		}
		panic("oops")
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		started2 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		switch workflow.GetInfo(ctx).Attempt {
		case 1:
			panic("oops")
		case 2:
			return "", errors.New("try again")
		case 3:
			return "done!", nil
		}
		panic("oops")
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

	// now register v2 as a new default
	rule = s.addAssignmentRule(ctx, tq, v2)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond)

	w2 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// unblock the workflow on v1
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	s.waitForChan(ctx, started2) // attempt 2
	// now it's blocked in attempt 2. unblock it.
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	// wait for attempt 3. unblock that and it should return.
	s.waitForChan(ctx, started2) // attempt 3
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v2, true, v2, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchCronOld() {
	s.testWithMatchingBehavior(func() { s.dispatchCron(false) })
}

func (s *VersioningIntegSuite) TestDispatchCron() {
	s.testWithMatchingBehavior(func() { s.dispatchCron(true) })
}

func (s *VersioningIntegSuite) dispatchCron(newVersioning bool) {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	v2 := s.prefixed("v2")

	var runIds1 []string
	var runIds2 []string

	var runs1 atomic.Int32
	var runs11 atomic.Int32
	var runs2 atomic.Int32

	wf1 := func(ctx workflow.Context) (string, error) {
		runs1.Add(1)
		runIds1 = append(runIds1, workflow.GetInfo(ctx).WorkflowExecution.RunID)
		return "ok", nil
	}
	wf11 := func(ctx workflow.Context) (string, error) {
		runs11.Add(1)
		return "ok", nil
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		runs2.Add(1)
		runIds2 = append(runIds2, workflow.GetInfo(ctx).WorkflowExecution.RunID)
		return "ok", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v1)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
	} else {
		s.addNewDefaultBuildId(ctx, tq, v1)
		s.waitForVersionSetPropagation(ctx, tq, v1)
	}

	w1 := worker.New(s.sdkClient, tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue:                tq,
		CronSchedule:             "@every 1s",
		WorkflowExecutionTimeout: 7 * time.Second,
	}, "wf")
	s.NoError(err)

	// give it ~3 runs on v1
	time.Sleep(3500 * time.Millisecond)

	if newVersioning {
		rule := s.addAssignmentRule(ctx, tq, v2)
		s.waitForAssignmentRulePropagation(ctx, tq, rule)
	} else {
		// now register v11 as newer compatible with v1 AND v2 as a new default.
		// it will run on v2 instead of v11 because cron always starts on default.
		s.addCompatibleBuildId(ctx, tq, v11, v1, false)
		s.addNewDefaultBuildId(ctx, tq, v2)
		s.waitForVersionSetPropagation(ctx, tq, v2)
	}

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

	for _, runid := range runIds1 {
		s.validateWorkflowBuildIds(ctx, run.GetID(), runid, v1, newVersioning, v1, "", nil)
	}
	for _, runid := range runIds2 {
		s.validateWorkflowBuildIds(ctx, run.GetID(), runid, v2, newVersioning, v2, "", nil)
	}
}

func (s *VersioningIntegSuite) TestResetWorkflowAssignsToCorrectBuildId() {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	act1 := func() (string, error) {
		return "act1 done!", nil
	}

	wf := func(ctx workflow.Context) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(
				ctx, workflow.ActivityOptions{
					DisableEagerExecution: true,
					StartToCloseTimeout:   1 * time.Second,
				},
			),
			act1,
		).Get(ctx, &ret)
		s.NoError(err)
		s.Equal("act1 done!", ret)
		return "done!", nil
	}

	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	w1 := worker.New(
		s.sdkClient, tq, worker.Options{
			BuildID:                          v1,
			UseBuildIDForVersioning:          true,
			MaxConcurrentWorkflowTaskPollers: numPollers,
		},
	)
	w1.RegisterWorkflow(wf)
	w1.RegisterActivity(act1)
	s.NoError(w1.Start())
	defer w1.Stop()
	w2 := worker.New(
		s.sdkClient, tq, worker.Options{
			BuildID:                          v2,
			UseBuildIDForVersioning:          true,
			MaxConcurrentWorkflowTaskPollers: numPollers,
		},
	)
	w2.RegisterWorkflow(wf)
	w2.RegisterActivity(act1)
	s.NoError(w2.Start())
	defer w2.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	s.validateBuildIdAfterReset(ctx, run.GetID(), run.GetRunID(), false)
}

func (s *VersioningIntegSuite) TestResetWorkflowAssignsToCorrectBuildId_CaN_Inherit() {
	s.resetWorkflowAssignsToCorrectBuildIdCan(true)
}

func (s *VersioningIntegSuite) TestResetWorkflowAssignsToCorrectBuildId_CaN_NoInherit() {
	s.resetWorkflowAssignsToCorrectBuildIdCan(false)
}

func (s *VersioningIntegSuite) resetWorkflowAssignsToCorrectBuildIdCan(inheritBuildId bool) {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	act1 := func() (string, error) {
		return "act1 done!", nil
	}

	wf := func(ctx workflow.Context, attempt int) (string, error) {
		switch attempt {
		case 1:
			intent := temporal.VersioningIntentDefault
			if inheritBuildId {
				intent = temporal.VersioningIntentCompatible
			}
			newCtx := workflow.WithWorkflowVersioningIntent(ctx, intent)
			return "", workflow.NewContinueAsNewError(newCtx, "wf", 2)
		case 2:
			if workflow.GetInfo(ctx).Attempt == 1 {
				// failing first attempt of the CaN so we test inherit behavior across retry attempts
				return "", errors.New("try again")
			}
			var ret string
			err := workflow.ExecuteActivity(
				workflow.WithActivityOptions(
					ctx, workflow.ActivityOptions{
						DisableEagerExecution: true,
						StartToCloseTimeout:   1 * time.Second,
					},
				),
				act1,
			).Get(ctx, &ret)
			s.NoError(err)
			s.Equal("act1 done!", ret)
			return "done!", nil
		default:
			panic("oops")
		}
	}

	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	w1 := worker.New(
		s.sdkClient, tq, worker.Options{
			BuildID:                          v1,
			UseBuildIDForVersioning:          true,
			MaxConcurrentWorkflowTaskPollers: numPollers,
		},
	)
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivity(act1)
	s.NoError(w1.Start())
	defer w1.Stop()
	w2 := worker.New(
		s.sdkClient, tq, worker.Options{
			BuildID:                          v2,
			UseBuildIDForVersioning:          true,
			MaxConcurrentWorkflowTaskPollers: numPollers,
		},
	)
	w2.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterActivity(act1)
	s.NoError(w2.Start())
	defer w2.Stop()

	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1000 * time.Millisecond,
		},
	}, wf, 1)
	s.NoError(err)
	s.validateBuildIdAfterReset(ctx, run.GetID(), "", inheritBuildId)
}

func (s *VersioningIntegSuite) TestResetWorkflowAssignsToCorrectBuildId_ChildWF_Inherit() {
	s.resetWorkflowAssignsToCorrectBuildIdChildWf(true)
}

func (s *VersioningIntegSuite) TestResetWorkflowAssignsToCorrectBuildId_ChildWF_NoInherit() {
	s.resetWorkflowAssignsToCorrectBuildIdChildWf(false)
}

func (s *VersioningIntegSuite) resetWorkflowAssignsToCorrectBuildIdChildWf(inheritBuildId bool) {
	tq := s.randomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	act1 := func() (string, error) {
		return "act1 done!", nil
	}

	var childWfId string
	childStarted := make(chan struct{})

	child := func(ctx workflow.Context) (string, error) {
		if workflow.GetInfo(ctx).Attempt == 1 {
			// failing first attempt of so we test inherit behavior across retry attempts
			return "", errors.New("try again")
		}
		var ret string
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(
				ctx, workflow.ActivityOptions{
					DisableEagerExecution: true,
					StartToCloseTimeout:   1 * time.Second,
				},
			),
			act1,
		).Get(ctx, &ret)
		s.NoError(err)
		s.Equal("act1 done!", ret)
		return "done!", nil
	}

	wf := func(ctx workflow.Context) (string, error) {
		intent := temporal.VersioningIntentDefault
		if inheritBuildId {
			intent = temporal.VersioningIntentCompatible
		}
		fut := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			VersioningIntent: intent,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval: 1000 * time.Millisecond,
			},
		}), child)
		var val string
		var childWE workflow.Execution
		s.NoError(fut.GetChildWorkflowExecution().Get(ctx, &childWE))
		childWfId = childWE.ID
		close(childStarted)
		s.NoError(fut.Get(ctx, &val))
		s.Equal("done!", val)
		return "parent done!", nil
	}

	rule := s.addAssignmentRule(ctx, tq, v1)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	w1 := worker.New(
		s.sdkClient, tq, worker.Options{
			BuildID:                          v1,
			UseBuildIDForVersioning:          true,
			MaxConcurrentWorkflowTaskPollers: numPollers,
		},
	)
	w1.RegisterWorkflow(wf)
	w1.RegisterWorkflow(child)
	w1.RegisterActivity(act1)
	s.NoError(w1.Start())
	defer w1.Stop()
	w2 := worker.New(
		s.sdkClient, tq, worker.Options{
			BuildID:                          v2,
			UseBuildIDForVersioning:          true,
			MaxConcurrentWorkflowTaskPollers: numPollers,
		},
	)
	w2.RegisterWorkflow(child)
	w2.RegisterActivity(act1)
	s.NoError(w2.Start())
	defer w2.Stop()

	_, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	s.waitForChan(ctx, childStarted)
	s.validateBuildIdAfterReset(ctx, childWfId, "", inheritBuildId)
}

// assumes given run has a single activity task.
func (s *VersioningIntegSuite) validateBuildIdAfterReset(ctx context.Context, wfId, runId string, expectedInherit bool) {
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	run := s.sdkClient.GetWorkflow(ctx, wfId, runId)

	// let the original run finish
	var out string
	s.NoError(run.Get(ctx, &out))
	s.Equal("done!", out)

	dw, err := s.sdkClient.DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
	s.NoError(err)
	tq := dw.GetWorkflowExecutionInfo().GetTaskQueue()
	inheritedBuildId := dw.GetWorkflowExecutionInfo().GetInheritedBuildId()
	s.Equal(expectedInherit, inheritedBuildId != "")
	s.validateWorkflowBuildIds(ctx, run.GetID(), run.GetRunID(), v1, true, v1, inheritedBuildId, nil)

	// update rules with v2 as the default build
	rule := s.addAssignmentRule(ctx, tq, v2)
	s.waitForAssignmentRulePropagation(ctx, tq, rule)

	// now reset the wf to first wf task
	wfr, err := s.sdkClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		},
		WorkflowTaskFinishEventId: 3,
	})
	s.NoError(err)

	// if a build ID is inherited, we should keep using that, otherwise should use the latest rules
	expectedBuildId := v2
	if inheritedBuildId != "" {
		expectedBuildId = inheritedBuildId
	}
	run2 := s.sdkClient.GetWorkflow(ctx, run.GetID(), wfr.GetRunId())
	s.NoError(run2.Get(ctx, &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(ctx, run2.GetID(), run2.GetRunID(), expectedBuildId, true, expectedBuildId, inheritedBuildId, nil)
	s.validateWorkflowEventsVersionStamps(ctx, run2.GetID(), run2.GetRunID(), []string{expectedBuildId, expectedBuildId, expectedBuildId}, inheritedBuildId)

	// now reset the original wf to second wf task and make sure it remains in v1
	wfr, err = s.sdkClient.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		},
		WorkflowTaskFinishEventId: 9,
	})
	s.NoError(err)

	run3 := s.sdkClient.GetWorkflow(ctx, run.GetID(), wfr.GetRunId())
	s.NoError(run3.Get(ctx, &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(ctx, run3.GetID(), run3.GetRunID(), v1, true, v1, inheritedBuildId, nil)
	s.validateWorkflowEventsVersionStamps(ctx, run3.GetID(), run3.GetRunID(), []string{v1, v1, v1}, inheritedBuildId)
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueEnhanced_Versioned_ReachabilityCache() {
	tq := s.randomizeStr(s.T().Name())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Add assignment rule A and start workflow with build id A
	s.addAssignmentRule(ctx, tq, "A")
	started := make(chan struct{}, 10)
	wf := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		if workflow.GetInfo(ctx).Attempt == 1 {
			return "", errors.New("try again")
		}
		panic("oops")
	}
	wId := s.randomizeStr("id")
	w := worker.New(s.sdkClient, tq, worker.Options{
		UseBuildIDForVersioning: true,
		BuildID:                 "A",
		Identity:                wId,
	})
	w.RegisterWorkflow(wf)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	s.waitForChan(ctx, started)

	// 2. Wait for A to show up as running in visibility db
	s.Eventually(func() bool {
		listResp, err := s.engine.ListOpenWorkflowExecutions(ctx, &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 10,
		})
		s.Nil(err)
		return len(listResp.GetExecutions()) > 0
	}, 5*time.Second, 50*time.Millisecond)

	// 3. Commit a different build id --> A should now only be reachable via visibility query
	s.commitBuildId(ctx, tq, "B", true, s.getVersioningRules(ctx, tq).GetConflictToken(), true)

	// 4. Query reachability(A) --> reachable by visibility db, populating reachability open WF cache with A: true
	s.getBuildIdReachability(ctx, tq, &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{"A"}}, map[string]enumspb.BuildIdTaskReachability{
		"A": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, // reachable by visibility (db)
	})

	// 5. Signal workflow with build id A so that it completes
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	// 6. Query reachability(A) --> eventually shows closed_only by visibility db (after TTL passes and A is closed in visibility)
	s.Eventually(func() bool {
		return s.checkBuildIdReachability(ctx, tq, &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{"A"}}, map[string]enumspb.BuildIdTaskReachability{
			"A": enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, // closed_only by visibility db (after TTL)
		})
	}, 5*time.Second, 50*time.Millisecond)
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueEnhanced_Versioned_BasicReachability() {
	tq := s.randomizeStr(s.T().Name())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.getBuildIdReachability(ctx, tq, nil, map[string]enumspb.BuildIdTaskReachability{
		"": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, // reachable because unversioned is default
	})

	s.addAssignmentRule(ctx, tq, "A")
	s.getBuildIdReachability(ctx, tq, &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{"", "A"}}, map[string]enumspb.BuildIdTaskReachability{
		"A": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE,   // reachable by default assignment rule
		"":  enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, // unreachable because no longer default
	})

	// start workflow and worker with new default assignment rule "A", and wait for it to start
	started := make(chan struct{}, 10)
	wf := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		if workflow.GetInfo(ctx).Attempt == 1 {
			return "", errors.New("try again")
		}
		panic("oops")
	}
	wId := s.randomizeStr("id")
	w := worker.New(s.sdkClient, tq, worker.Options{
		UseBuildIDForVersioning: true,
		BuildID:                 "A",
		Identity:                wId,
	})
	w.RegisterWorkflow(wf)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := s.sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	s.waitForChan(ctx, started)

	// wait for visibility to show A as started
	s.Eventually(func() bool {
		listResp, err := s.engine.ListOpenWorkflowExecutions(ctx, &workflowservice.ListOpenWorkflowExecutionsRequest{
			Namespace:       s.namespace,
			MaximumPageSize: 10,
		})
		s.Nil(err)
		return len(listResp.GetExecutions()) > 0
	}, 3*time.Second, 50*time.Millisecond)

	// commit a different build ID --> A should now only be reachable via visibility query, B reachable as default
	s.commitBuildId(ctx, tq, "B", true, s.getVersioningRules(ctx, tq).GetConflictToken(), true)
	s.getBuildIdReachability(ctx, tq, nil, map[string]enumspb.BuildIdTaskReachability{
		"B": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, // reachable by default assignment rule
	})
	s.getBuildIdReachability(ctx, tq, &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{"A", "B"}}, map[string]enumspb.BuildIdTaskReachability{
		"A": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, // reachable by visibility
		"B": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, // reachable by default assignment rule
	})

	// unblock the workflow on A so that it closes
	s.NoError(s.sdkClient.SignalWorkflow(ctx, run.GetID(), "", "wait", nil))

	// Query reachability(A) --> eventually shows closed_only by visibility db (after TTL passes and A is closed in visibility)
	s.Eventually(func() bool {
		return s.checkBuildIdReachability(ctx, tq, &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{"A"}}, map[string]enumspb.BuildIdTaskReachability{
			"A": enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, // closed_only by visibility db (after TTL)
			"B": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE,             // reachable by default assignment rule
		})
	}, 5*time.Second, 50*time.Millisecond)
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueEnhanced_Unversioned() {
	tq := s.randomizeStr(s.T().Name())
	wf := func(ctx workflow.Context) (string, error) { return "ok", nil }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerN := 3
	workerMap := make(map[string]worker.Worker)
	for i := 0; i < workerN; i++ {
		wId := s.randomizeStr("id")
		w := worker.New(s.sdkClient, tq, worker.Options{
			UseBuildIDForVersioning: false,
			Identity:                wId,
		})
		w.RegisterWorkflow(wf)
		s.NoError(w.Start())
		defer w.Stop()
		workerMap[wId] = w
	}

	s.Eventually(func() bool {
		resp, err := s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:              s.namespace,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			Versions:               nil, // default version, in this case unversioned queue
			TaskQueueTypes:         nil, // both types
			ReportPollers:          true,
			ReportTaskReachability: true,
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Assert().Equal(1, len(resp.GetVersionsInfo()), "should be 1 because only default/unversioned queue")
		versionInfo := resp.GetVersionsInfo()[""]
		s.Assert().Equal(enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, versionInfo.GetTaskReachability())
		var pollersInfo []*taskqueuepb.PollerInfo
		for _, t := range versionInfo.GetTypesInfo() {
			pollersInfo = append(pollersInfo, t.GetPollers()...)
		}
		foundN := 0
		for wId := range workerMap {
			for _, pi := range pollersInfo {
				s.False(pi.GetWorkerVersionCapabilities().GetUseVersioning())
				if pi.GetIdentity() == wId {
					foundN++
					break
				}
			}
		}

		return foundN == workerN
	}, 3*time.Second, 50*time.Millisecond)
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueEnhanced_ReportFlags() {
	tq := s.randomizeStr(s.T().Name())
	wf := func(ctx workflow.Context) (string, error) { return "ok", nil }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wId := s.randomizeStr("id")
	w := worker.New(s.sdkClient, tq, worker.Options{
		UseBuildIDForVersioning: false,
		Identity:                wId,
	})
	w.RegisterWorkflow(wf)
	s.NoError(w.Start())
	defer w.Stop()

	// wait for pollers to show up, verify both ReportPollers and ReportTaskReachability
	s.Eventually(func() bool {
		resp, err := s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace:              s.namespace,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			Versions:               nil, // default version, in this case unversioned queue
			TaskQueueTypes:         nil, // both types
			ReportPollers:          true,
			ReportTaskReachability: true,
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Assert().Equal(1, len(resp.GetVersionsInfo()), "should be 1 because only default/unversioned queue")
		versionInfo := resp.GetVersionsInfo()[""]
		s.Assert().Equal(enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, versionInfo.GetTaskReachability())
		var pollersInfo []*taskqueuepb.PollerInfo
		for _, t := range versionInfo.GetTypesInfo() {
			pollersInfo = append(pollersInfo, t.GetPollers()...)
		}
		for _, pi := range pollersInfo {
			s.False(pi.GetWorkerVersionCapabilities().GetUseVersioning())
			if pi.GetIdentity() == wId {
				return true
			}
		}

		return false
	}, 3*time.Second, 50*time.Millisecond)

	// ask for reachability only
	resp, err := s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:              s.namespace,
		TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:               nil, // default version, in this case unversioned queue
		TaskQueueTypes:         nil, // both types
		ReportTaskReachability: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.Assert().Equal(1, len(resp.GetVersionsInfo()), "should be 1 because only default/unversioned queue")
	versionInfo := resp.GetVersionsInfo()[""]
	s.Assert().Equal(enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, versionInfo.GetTaskReachability())
	for _, t := range versionInfo.GetTypesInfo() {
		s.Zero(len(t.GetPollers()), "poller info should not be reported")
	}

	// ask for pollers only
	resp, err = s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:      s.namespace,
		TaskQueue:      &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:        enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:       nil, // default version, in this case unversioned queue
		TaskQueueTypes: nil, // both types
		ReportPollers:  true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.Assert().Equal(1, len(resp.GetVersionsInfo()), "should be 1 because only default/unversioned queue")
	versionInfo = resp.GetVersionsInfo()[""]
	s.Assert().Equal(enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, versionInfo.GetTaskReachability())
	for _, t := range versionInfo.GetTypesInfo() {
		s.Equal(1, len(t.GetPollers()), "only one poller info should be reported")
	}
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueEnhanced_TooManyBuildIds() {
	tq := s.randomizeStr(s.T().Name())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	buildIds := []string{"A", "B", "C", "D"}
	resp, err := s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:              s.namespace,
		TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:               &taskqueuepb.TaskQueueVersionSelection{BuildIds: buildIds},
		TaskQueueTypes:         nil, // both types
		ReportPollers:          false,
		ReportTaskReachability: true,
	})
	s.NoError(err)
	s.NotNil(resp)

	buildIds = []string{"A", "B", "C", "D", "E"}
	resp, err = s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:              s.namespace,
		TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:               &taskqueuepb.TaskQueueVersionSelection{BuildIds: buildIds},
		TaskQueueTypes:         nil, // both types
		ReportPollers:          false,
		ReportTaskReachability: true,
	})
	s.Error(err)
	s.Nil(resp)
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueLegacy_VersionSets() {
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

	// describe and check build ID
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

// Add a per test prefix to avoid hitting the namespace limit of mapped task queue per build ID
func (s *VersioningIntegSuite) prefixed(buildId string) string {
	return fmt.Sprintf("t%x:%s", 0xffff&farm.Hash32([]byte(s.T().Name())), buildId)
}

// listVersioningRules lists rules and checks that the result is successful, returning the response.
func (s *VersioningIntegSuite) getVersioningRules(
	ctx context.Context, tq string) *workflowservice.GetWorkerVersioningRulesResponse {
	res, err := s.engine.GetWorkerVersioningRules(ctx, &workflowservice.GetWorkerVersioningRulesRequest{
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
		s.Assert().Equal(newBuildId, res.GetAssignmentRules()[idx].GetRule().GetTargetBuildId())
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
		s.Assert().Equal(newBuildId, res.GetAssignmentRules()[idx].GetRule().GetTargetBuildId())
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
	getResp, err := s.engine.GetWorkerVersioningRules(ctx, &workflowservice.GetWorkerVersioningRulesRequest{
		Namespace: s.namespace,
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(getResp)

	var prevRule *taskqueuepb.BuildIdAssignmentRule
	if expectSuccess {
		prevRule = getResp.GetAssignmentRules()[idx].GetRule()
	}

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
		found := false
		for _, r := range res.GetAssignmentRules() {
			if r.GetRule() == prevRule {
				found = true
				break
			}
		}
		s.Assert().False(found)
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
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleRedirectRule{
			AddCompatibleRedirectRule: &workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule{
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
		found := false
		for _, r := range res.GetCompatibleRedirectRules() {
			if r.GetRule().GetSourceBuildId() == sourceBuildId && r.GetRule().GetTargetBuildId() == targetBuildId {
				found = true
				break
			}
		}
		s.Assert().True(found)
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
		found := false
		for _, r := range res.GetCompatibleRedirectRules() {
			if r.GetRule().GetSourceBuildId() == sourceBuildId && r.GetRule().GetTargetBuildId() == targetBuildId {
				found = true
				break
			}
		}
		s.Assert().True(found)
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
		found := false
		for _, r := range res.GetCompatibleRedirectRules() {
			if r.GetRule().GetSourceBuildId() == sourceBuildId {
				found = true
				break
			}
		}
		s.Assert().False(found)
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
		// 1. Adds an assignment rule (with full ramp) for the target Build ID at the end of the list.
		endIdx := len(res.GetAssignmentRules()) - 1
		addedRule := res.GetAssignmentRules()[endIdx].GetRule()
		s.Assert().Equal(targetBuildId, addedRule.GetTargetBuildId())
		s.Assert().Nil(addedRule.GetRamp())
		s.Assert().Nil(addedRule.GetPercentageRamp())

		foundOtherAssignmentRuleForTarget := false
		foundFullyRampedAssignmentRuleForOtherTarget := false
		for i, r := range res.GetAssignmentRules() {
			if r.GetRule().GetTargetBuildId() == targetBuildId && i != endIdx {
				foundOtherAssignmentRuleForTarget = true
			}
			if r.GetRule().GetRamp() == nil && r.GetRule().GetTargetBuildId() != targetBuildId {
				foundFullyRampedAssignmentRuleForOtherTarget = true
			}
		}
		// 2. Removes all previously added assignment rules to the given target Build ID (if any).
		s.Assert().False(foundOtherAssignmentRuleForTarget)
		// 3. Removes any fully-ramped assignment rule for other Build IDs.
		s.Assert().False(foundFullyRampedAssignmentRuleForOtherTarget)
		return res.GetConflictToken()
	} else {
		s.Error(err)
		s.Nil(res)
		return nil
	}
}

func (s *VersioningIntegSuite) registerWorkflowAndPollVersionedTaskQueue(tq, buildID string, useVersioning bool) {
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

func (s *VersioningIntegSuite) getBuildIdReachability(
	ctx context.Context,
	taskQueue string,
	versions *taskqueuepb.TaskQueueVersionSelection,
	expectedReachability map[string]enumspb.BuildIdTaskReachability) {
	resp, err := s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:              s.namespace,
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:               versions,
		TaskQueueTypes:         nil, // both types
		ReportPollers:          false,
		ReportTaskReachability: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	for buildId, vi := range resp.GetVersionsInfo() {
		expected, ok := expectedReachability[buildId]
		s.Assert().True(ok, "build id %s was not expected", buildId)
		s.Assert().Equal(expected, vi.GetTaskReachability(), "build id %s has unexpected reachability", buildId)
	}
}

func (s *VersioningIntegSuite) checkBuildIdReachability(
	ctx context.Context,
	taskQueue string,
	versions *taskqueuepb.TaskQueueVersionSelection,
	expectedReachability map[string]enumspb.BuildIdTaskReachability) bool {
	resp, err := s.engine.DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:              s.namespace,
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:               versions,
		TaskQueueTypes:         nil, // both types
		ReportPollers:          false,
		ReportTaskReachability: true,
	})
	if err != nil {
		return false
	}
	if resp == nil {
		return false
	}
	for buildId, vi := range resp.GetVersionsInfo() {
		expected, ok := expectedReachability[buildId]
		if !ok {
			return false // build id was not expected
		}
		if expected != vi.GetTaskReachability() {
			return false // build id has unexpected reachability
		}
	}
	return true
}

// addNewDefaultBuildId updates build ID info on a task queue with a new build ID in a new default set.
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
	rule := &taskqueuepb.BuildIdAssignmentRule{
		TargetBuildId: buildId,
	}
	return s.doAddAssignmentRule(ctx, tq, rule)
}

func (s *VersioningIntegSuite) addAssignmentRuleWithRamp(ctx context.Context, tq, buildId string, ramp float32) *taskqueuepb.BuildIdAssignmentRule {
	rule := &taskqueuepb.BuildIdAssignmentRule{
		TargetBuildId: buildId,
		Ramp: &taskqueuepb.BuildIdAssignmentRule_PercentageRamp{
			PercentageRamp: &taskqueuepb.RampByPercentage{
				RampPercentage: ramp,
			},
		},
	}
	return s.doAddAssignmentRule(ctx, tq, rule)
}

func (s *VersioningIntegSuite) doAddAssignmentRule(ctx context.Context, tq string, rule *taskqueuepb.BuildIdAssignmentRule) *taskqueuepb.BuildIdAssignmentRule {
	cT := s.getVersioningRules(ctx, tq).GetConflictToken()
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

func (s *VersioningIntegSuite) addRedirectRule(ctx context.Context, tq, source string, target string) *taskqueuepb.CompatibleBuildIdRedirectRule {
	cT := s.getVersioningRules(ctx, tq).GetConflictToken()
	rule := &taskqueuepb.CompatibleBuildIdRedirectRule{
		SourceBuildId: source,
		TargetBuildId: target,
	}
	res, err := s.engine.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     s.namespace,
		TaskQueue:     tq,
		ConflictToken: cT,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleRedirectRule{
			AddCompatibleRedirectRule: &workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule{
				Rule: rule,
			},
		},
	})
	s.NoError(err)
	s.NotNil(res)
	return rule
}

func (s *VersioningIntegSuite) removeRedirectRule(ctx context.Context, tq, source string) {
	cT := s.getVersioningRules(ctx, tq).GetConflictToken()
	res, err := s.engine.UpdateWorkerVersioningRules(ctx, &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     s.namespace,
		TaskQueue:     tq,
		ConflictToken: cT,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleRedirectRule{
			DeleteCompatibleRedirectRule: &workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule{
				SourceBuildId: source,
			},
		},
	})
	s.NoError(err)
	s.NotNil(res)
}

// addCompatibleBuildId updates build ID info on a task queue with a new compatible build ID.
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
	s.waitForPropagation(ctx, taskQueue, 0, func(vd *persistencespb.VersioningData) bool {
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
	s.waitForPropagation(ctx, taskQueue, 0, func(vd *persistencespb.VersioningData) bool {
		for _, r := range vd.GetAssignmentRules() {
			if r.GetRule().Equal(rule) {
				return true
			}
		}
		return false
	})
}

// waitForRedirectRulePropagation waits for all partitions of tq to have the given redirect rule in their versioning data
func (s *VersioningIntegSuite) waitForRedirectRulePropagation(ctx context.Context, taskQueue string, rule *taskqueuepb.CompatibleBuildIdRedirectRule) {
	s.waitForPropagation(ctx, taskQueue, 0, func(vd *persistencespb.VersioningData) bool {
		for _, r := range vd.GetRedirectRules() {
			if r.GetRule().Equal(rule) {
				return true
			}
		}
		return false
	})
}

// waitForRedirectRulePropagationUpToPartition waits for partitions of tq, up to a certain partition ID, to have the
// given redirect rule in their versioning data
func (s *VersioningIntegSuite) waitForRedirectRulePropagationUpToPartition(
	ctx context.Context,
	taskQueue string,
	rule *taskqueuepb.CompatibleBuildIdRedirectRule,
	upToPartition int,
) {
	s.waitForPropagation(ctx, taskQueue, upToPartition+1, func(vd *persistencespb.VersioningData) bool {
		for _, r := range vd.GetRedirectRules() {
			if r.GetRule().Equal(rule) {
				return true
			}
		}
		return false
	})
}

// waitForPropagation waits for all partitions of tq to mention newBuildId in their versioning data (in any position).
// Pass 0 for partitionCount to make it load from MatchingNumTaskqueueReadPartitions config.
func (s *VersioningIntegSuite) waitForPropagation(
	ctx context.Context,
	taskQueue string,
	partitionCount int,
	condition func(data *persistencespb.VersioningData) bool,
) {
	if partitionCount <= 0 {
		v, ok := s.testCluster.host.dcClient.getRawValue(dynamicconfig.MatchingNumTaskqueueReadPartitions.Key())
		s.True(ok, "versioning tests require setting explicit number of partitions")
		partitionCount, ok = v.(int)
		s.True(ok, "partition count is not an int")
	}

	type partAndType struct {
		part int
		tp   enumspb.TaskQueueType
	}
	remaining := make(map[partAndType]struct{})
	for i := 0; i < partitionCount; i++ {
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

// Periodically checks a WF and unblocks when it is assigned to the given build ID
func (s *VersioningIntegSuite) waitForWorkflowBuildId(
	ctx context.Context,
	wfId string,
	runId string,
	buildId string,
) {
	s.Eventually(
		func() bool {
			dw, err := s.sdkClient.DescribeWorkflowExecution(ctx, wfId, runId)
			if err != nil {
				return false
			}
			return dw.GetWorkflowExecutionInfo().GetAssignedBuildId() == buildId
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func (s *VersioningIntegSuite) validateWorkflowBuildIds(
	ctx context.Context,
	wfId string,
	runId string,
	expectedBuildId string,
	newVersioning bool,
	expectedStampBuildId string,
	expectedInheritedBuildId string,
	extraSearchAttrBuildIds []string,
) {
	dw, err := s.sdkClient.DescribeWorkflowExecution(ctx, wfId, runId)
	s.NoError(err)
	saPayload := dw.GetWorkflowExecutionInfo().GetSearchAttributes().GetIndexedFields()["BuildIds"]
	searchAttrAny, err := searchattribute.DecodeValue(saPayload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, true)
	var searchAttr []string
	if searchAttrAny != nil {
		searchAttr = searchAttrAny.([]string)
	}
	s.NoError(err)
	if expectedBuildId == "" {
		if expectedStampBuildId != "" {
			s.NotNil(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId())
			s.False(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetUseVersioning())
			s.Equal(2+len(extraSearchAttrBuildIds), len(searchAttr))
			s.Equal(worker_versioning.UnversionedSearchAttribute, searchAttr[0])
			s.True(strings.HasPrefix(searchAttr[1], worker_versioning.UnversionedSearchAttribute))
		} else {
			s.Nil(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp())
			s.Equal(0, len(searchAttr))
		}
	} else {
		if expectedStampBuildId != "" {
			s.True(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetUseVersioning())
			s.Equal(expectedStampBuildId, dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId())
		} else {
			s.Nil(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp())
		}
		if newVersioning {
			s.Equal(expectedBuildId, dw.GetWorkflowExecutionInfo().GetAssignedBuildId())
			s.Equal(2+len(extraSearchAttrBuildIds), len(searchAttr))
			s.Equal(worker_versioning.AssignedBuildIdSearchAttribute(expectedBuildId), searchAttr[0])
			s.Contains(searchAttr, worker_versioning.VersionedBuildIdSearchAttribute(expectedBuildId))
		} else {
			s.Equal("", dw.GetWorkflowExecutionInfo().GetAssignedBuildId())
			if expectedStampBuildId != "" {
				s.Equal(1+len(extraSearchAttrBuildIds), len(searchAttr))
				s.Contains(searchAttr, worker_versioning.VersionedBuildIdSearchAttribute(expectedBuildId))
			} else {
				s.Equal(0, len(searchAttr))
			}
		}
	}
	s.Equal(expectedInheritedBuildId, dw.GetWorkflowExecutionInfo().GetInheritedBuildId())
	for _, b := range extraSearchAttrBuildIds {
		if expectedBuildId == "" {
			s.Contains(searchAttr, worker_versioning.UnversionedBuildIdSearchAttribute(b))
		} else {
			s.Contains(searchAttr, worker_versioning.VersionedBuildIdSearchAttribute(b))
		}
	}
}

func (s *VersioningIntegSuite) validateWorkflowEventsVersionStamps(
	ctx context.Context,
	wfId, runId string,
	expectedBuildIds []string,
	expectedInheritedBuildId string,
) {
	wh := s.sdkClient.GetWorkflowHistory(ctx, wfId, runId, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	counter := 0
	checkedInheritedBuildId := false
	for wh.HasNext() {
		he, err := wh.Next()
		s.Nil(err)
		if !checkedInheritedBuildId {
			// first event
			checkedInheritedBuildId = true
			s.Equal(expectedInheritedBuildId, he.GetWorkflowExecutionStartedEventAttributes().GetInheritedBuildId())
		}
		var taskStartedStamp *commonpb.WorkerVersionStamp
		if activityStarted := he.GetActivityTaskStartedEventAttributes(); activityStarted != nil {
			taskStartedStamp = activityStarted.GetWorkerVersion()
		} else if wfStarted := he.GetWorkflowTaskStartedEventAttributes(); wfStarted != nil {
			taskStartedStamp = wfStarted.GetWorkerVersion()
		}
		if taskStartedStamp != nil {
			if counter >= len(expectedBuildIds) {
				s.Fail("found more task started events than expected")
			}
			expected := expectedBuildIds[counter]
			if expected == "" {
				s.False(taskStartedStamp.GetUseVersioning())
			} else {
				s.True(taskStartedStamp.GetUseVersioning())
				s.Equal(expected, taskStartedStamp.GetBuildId())
			}
			counter++
		}
	}
	if counter != len(expectedBuildIds) {
		s.Fail("found less task started events than expected")
	}
}
