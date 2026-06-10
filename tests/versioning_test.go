// nolint:revive
package tests

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
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
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type VersioningIntegSuite struct {
	parallelsuite.Suite[*VersioningIntegSuite]
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

func TestVersioningFunctionalSuite(t *testing.T) {
	parallelsuite.Run(t, &VersioningIntegSuite{})
}

func (s *VersioningIntegSuite) setupEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	opts = append([]testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableDeployments, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningDataAPIs, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningWorkflowAPIs, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendEnableWorkerVersioningRuleAPIs, true),
		testcore.WithDynamicConfig(dynamicconfig.TaskQueuesPerBuildIdLimit, 3),
		testcore.WithDynamicConfig(dynamicconfig.EnableWorkflowTaskStampIncrementOnFailure, true),

		testcore.WithDynamicConfig(dynamicconfig.AssignmentRuleLimitPerQueue, 10),
		testcore.WithDynamicConfig(dynamicconfig.RedirectRuleLimitPerQueue, 10),
		testcore.WithDynamicConfig(dynamicconfig.RedirectRuleMaxUpstreamBuildIDsPerQueue, 10),
		testcore.WithDynamicConfig(dynamicconfig.MatchingDeletedRuleRetentionTime, 24*time.Hour),
		testcore.WithDynamicConfig(dynamicconfig.ReachabilityBuildIdVisibilityGracePeriod, 3*time.Minute),
		testcore.WithDynamicConfig(dynamicconfig.ReachabilityQueryBuildIdLimit, 4),
		testcore.WithDynamicConfig(dynamicconfig.ReachabilityCacheOpenWFsTTL, testReachabilityCacheOpenWFsTTL),
		testcore.WithDynamicConfig(dynamicconfig.ReachabilityCacheClosedWFsTTL, testReachabilityCacheClosedWFsTTL),

		// Make sure we don't hit the rate limiter in tests
		testcore.WithDynamicConfig(dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS, 1000),
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance, 1),
		testcore.WithDynamicConfig(dynamicconfig.FrontendNamespaceReplicationInducingAPIsRPS, 1000),

		// The dispatch tests below rely on being able to see the effects of changing
		// versioning data relatively quickly. In general, we only promise to act on new
		// versioning data "soon", i.e. after a long poll interval. We can reduce the long poll
		// interval so that we don't have to wait so long.
		// TODO: after cleaning up old versioning we may not need to depend on long poll time. [cleanup-old-wv]
		// This is because we don't interrupt long polls on change of versioning
		// data, so if polls from different build ids go to the same matcher, old ones can steal
		// tasks from new ones. If polls from different build ids never go to the same matcher
		// anymore then we don't need it.
		testcore.WithDynamicConfig(dynamicconfig.MatchingLongPollExpirationInterval, longPollTime),

		// this is overridden for tests using testWithMatchingBehavior
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4),

		// this is overridden since we don't want caching to be enabled while testing DescribeTaskQueue
		// behaviour related to versioning
		testcore.WithDynamicConfig(dynamicconfig.TaskQueueInfoByBuildIdTTL, 0*time.Second),
	}, opts...)

	return testcore.NewEnv(s.T(), opts...)
}

func (s *VersioningIntegSuite) runTestWithMatchingBehavior(subtest func(*testcore.TestEnv, *VersioningIntegSuite)) {
	for _, behavior := range testcore.AllMatchingBehaviors() {
		behavior := behavior
		s.Run(behavior.Name(), func(s *VersioningIntegSuite) {
			env := s.setupEnv(behavior.Options()...)
			behavior.InjectHooks(env)
			subtest(env, s)
		})
	}
}

func (s *VersioningIntegSuite) TestVersionRuleConflictToken() {
	env := s.setupEnv()
	tq := "test-conflict-token"

	// nil token --> fail
	s.insertAssignmentRule(env, tq, "1", 0, nil, false)

	// correct token from List --> success
	cT1 := s.getVersioningRules(env, tq).GetConflictToken()
	cT2 := s.insertAssignmentRule(env, tq, "2", 0, cT1, true)

	// confirm token changed on insert but not on list
	cT3 := s.getVersioningRules(env, tq).GetConflictToken()
	s.NotEqual(cT1, cT2)
	s.Equal(cT2, cT3)

	// correct token from List or most recent mutation --> success
	cT4 := s.insertAssignmentRule(env, tq, "3", 0, cT2, true)
	s.getVersioningRules(env, tq)

	// wrong token fails, same request with nil token also fails
	s.insertAssignmentRule(env, tq, "4", 0, cT1, false)
	s.insertAssignmentRule(env, tq, "4", 0, nil, false)

	// wrong token fails, same request with correct token from Update succeeds
	s.replaceAssignmentRule(env, tq, "20", 0, cT2, false)
	cT5 := s.replaceAssignmentRule(env, tq, "20", 0, cT4, true)
	cT6 := s.getVersioningRules(env, tq).GetConflictToken()

	// confirm that list didn't change the conflict token again
	s.Equal(cT5, cT6)

	// wrong token fails, same request with correct token from List succeeds
	s.deleteAssignmentRule(env, tq, 0, cT4, false)
	s.deleteAssignmentRule(env, tq, 0, cT6, true)

	// nil token fails
	s.deleteAssignmentRule(env, tq, 0, nil, false)
}

func (s *VersioningIntegSuite) TestVersionRuleBuildIdValidation() {
	// setup
	env := s.setupEnv()
	tq := "test-build-id-validation"

	// get initial conflict token
	cT := s.getVersioningRules(env, tq).GetConflictToken()

	// failure due to long build id
	longStr := strings.Repeat("0123456789", 30)
	s.insertAssignmentRule(env, tq, longStr, 0, cT, false)
	s.insertRedirectRule(env, tq, longStr, "foo", cT, false)
	s.insertRedirectRule(env, tq, "foo", longStr, cT, false)
}

func (s *VersioningIntegSuite) TestAssignmentRuleInsert() {
	// setup
	env := s.setupEnv()
	tq := "test-assignment-rule-insert"

	// get initial conflict token
	cT := s.getVersioningRules(env, tq).GetConflictToken()

	// success
	cT = s.insertAssignmentRule(env, tq, "1", 0, cT, true)
	res1 := s.getVersioningRules(env, tq)
	s.Equal("1", res1.GetAssignmentRules()[0].GetRule().GetTargetBuildId())

	// failure due to out of bounds index
	s.insertAssignmentRule(env, tq, "2", -1, cT, false)
	s.Equal(res1, s.getVersioningRules(env, tq))

	// success with conflict token returned by last successful call, same as above
	s.insertAssignmentRule(env, tq, "2", 1, cT, true)
	s.Equal("2", s.getVersioningRules(env, tq).GetAssignmentRules()[1].GetRule().GetTargetBuildId())
}

func (s *VersioningIntegSuite) TestAssignmentRuleReplace() {
	// setup
	env := s.setupEnv()
	tq := "test-assignment-rule-replace"

	// get initial conflict token + do initial inserts
	cT := s.getVersioningRules(env, tq).GetConflictToken()
	cT = s.insertAssignmentRule(env, tq, "1", 0, cT, true)
	cT = s.insertAssignmentRule(env, tq, "2", 0, cT, true)

	// success
	cT = s.replaceAssignmentRule(env, tq, "3", 0, cT, true)
	res := s.getVersioningRules(env, tq)
	s.Equal("3", res.GetAssignmentRules()[0].GetRule().GetTargetBuildId())

	// failure due to index out of bounds
	s.replaceAssignmentRule(env, tq, "4", 10, cT, false)
	s.Equal(res, s.getVersioningRules(env, tq))

	// success with conflict token returned by last successful call, same as above
	s.replaceAssignmentRule(env, tq, "4", 0, cT, true)
	s.Equal("4", s.getVersioningRules(env, tq).GetAssignmentRules()[0].GetRule().GetTargetBuildId())
}

func (s *VersioningIntegSuite) TestAssignmentRuleDelete() {
	// setup
	env := s.setupEnv()
	tq := "test-assignment-rule-delete"

	// get initial conflict token + do initial inserts
	cT := s.getVersioningRules(env, tq).GetConflictToken()
	cT = s.insertAssignmentRule(env, tq, "1", 0, cT, true)
	cT = s.insertAssignmentRule(env, tq, "2", 0, cT, true)

	// success
	cT = s.deleteAssignmentRule(env, tq, 0, cT, true)
	res := s.getVersioningRules(env, tq)
	s.Len(res.GetAssignmentRules(), 1)

	// failure due to requirement that once a fully-ramped rule exists, at least one must always exist
	s.deleteAssignmentRule(env, tq, 0, cT, false)
	s.Equal(res, s.getVersioningRules(env, tq))

	// insert another rule to prove that the conflict token was not the issue above
	cT = s.insertAssignmentRule(env, tq, "2", 0, cT, true)

	// delete again, success
	s.deleteAssignmentRule(env, tq, 0, cT, true)
	s.Len(res.GetAssignmentRules(), 1)

}

func (s *VersioningIntegSuite) TestRedirectRuleInsert() {
	// setup
	env := s.setupEnv()
	tq := "test-redirect-rule-insert"

	// get initial conflict token
	cT := s.getVersioningRules(env, tq).GetConflictToken()

	// success
	cT = s.insertRedirectRule(env, tq, "1", "0", cT, true)
	res := s.getVersioningRules(env, tq)
	rulesMap := mkRedirectRulesMap(res.GetCompatibleRedirectRules())
	s.Contains(rulesMap, "1")
	s.Equal("0", rulesMap["1"])

	// failure due to cycle
	s.insertRedirectRule(env, tq, "0", "1", cT, false)
	s.Equal(res, s.getVersioningRules(env, tq))

	// success with same conflict token but no cycle
	s.insertRedirectRule(env, tq, "0", "2", cT, true)
}

func (s *VersioningIntegSuite) TestRedirectRuleReplace() {
	// setup
	env := s.setupEnv()
	tq := "test-redirect-rule-replace"

	// get initial conflict token + do initial insert
	cT := s.getVersioningRules(env, tq).GetConflictToken()
	cT = s.insertRedirectRule(env, tq, "1", "0", cT, true)

	// success
	cT = s.replaceRedirectRule(env, tq, "1", "2", cT, true)
	res := s.getVersioningRules(env, tq)
	rulesMap := mkRedirectRulesMap(res.GetCompatibleRedirectRules())
	s.Contains(rulesMap, "1")
	s.Equal("2", rulesMap["1"])

	// failure due to source not found
	s.replaceRedirectRule(env, tq, "10", "3", cT, false)
	s.Equal(res, s.getVersioningRules(env, tq))

	// success with same conflict token and correct source
	s.replaceRedirectRule(env, tq, "1", "3", cT, true)
}

func (s *VersioningIntegSuite) TestRedirectRuleDelete() {
	// setup
	env := s.setupEnv()
	tq := "test-redirect-rule-delete"

	// get initial conflict token + do initial inserts
	cT := s.getVersioningRules(env, tq).GetConflictToken()
	cT = s.insertRedirectRule(env, tq, "1", "0", cT, true)
	cT = s.insertRedirectRule(env, tq, "2", "0", cT, true)

	// success
	cT = s.deleteRedirectRule(env, tq, "1", cT, true)
	res := s.getVersioningRules(env, tq)
	s.Len(res.GetCompatibleRedirectRules(), 1)

	// failure due to source not found
	s.deleteRedirectRule(env, tq, "1", cT, false)
	s.Equal(res, s.getVersioningRules(env, tq))

	// success with same conflict token and valid source
	s.deleteRedirectRule(env, tq, "2", cT, true)
}

func (s *VersioningIntegSuite) TestCommitBuildID() {
	// setup
	env := s.setupEnv()
	tq := "test-commit-build-id"

	// get initial conflict token
	cT := s.getVersioningRules(env, tq).GetConflictToken()

	// no recent poller --> failure
	s.commitBuildID(env, tq, "1", false, cT, false)

	// no recent poller + force --> success
	cT = s.commitBuildID(env, tq, "1", true, cT, true)
	res := s.getVersioningRules(env, tq)
	s.Len(res.GetAssignmentRules(), 1)
	s.Empty(res.GetCompatibleRedirectRules())
	s.Equal("1", res.GetAssignmentRules()[0].GetRule().GetTargetBuildId())
	s.Equal(float32(100), res.GetAssignmentRules()[0].GetRule().GetPercentageRamp().GetRampPercentage())

	// recent versioned poller on wrong build ID --> failure
	s.registerWorkflowAndPollVersionedTaskQueue(env, tq, "3", true)
	s.commitBuildID(env, tq, "2", false, cT, false)

	// recent unversioned poller on build ID 2 --> failure
	s.registerWorkflowAndPollVersionedTaskQueue(env, tq, "2", false)
	s.commitBuildID(env, tq, "2", false, cT, false)

	// recent versioned poller on build ID 2 --> success
	s.registerWorkflowAndPollVersionedTaskQueue(env, tq, "2", true)
	s.commitBuildID(env, tq, "2", false, cT, true)
	res = s.getVersioningRules(env, tq)
	s.Len(res.GetAssignmentRules(), 1)
	s.Empty(res.GetCompatibleRedirectRules())
	s.Equal("2", res.GetAssignmentRules()[0].GetRule().GetTargetBuildId())
	s.Equal(float32(100), res.GetAssignmentRules()[0].GetRule().GetPercentageRamp().GetRampPercentage())
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
	env := s.setupEnv()
	tq := "functional-versioning-basic"

	foo := s.prefixed("foo")
	s.addNewDefaultBuildID(env, tq, foo)

	res2, err := env.FrontendClient().GetWorkerBuildIdCompatibility(s.Context(), &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res2)
	s.Equal(foo, getCurrentDefault(res2))
}

func (s *VersioningIntegSuite) TestSeriesOfUpdates() {
	env := s.setupEnv()
	tq := "functional-versioning-series"

	for i := range 10 {
		s.addNewDefaultBuildID(env, tq, s.prefixed(fmt.Sprintf("foo-%d", i)))
	}
	s.addCompatibleBuildID(env, tq, s.prefixed("foo-2.1"), s.prefixed("foo-2"), false)

	res, err := env.FrontendClient().GetWorkerBuildIdCompatibility(s.Context(), &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res)
	s.Equal(s.prefixed("foo-9"), getCurrentDefault(res))
	s.Equal(s.prefixed("foo-2.1"), res.GetMajorVersionSets()[2].GetBuildIds()[1])
	s.Equal(s.prefixed("foo-2"), res.GetMajorVersionSets()[2].GetBuildIds()[0])
}

func (s *VersioningIntegSuite) TestLinkToNonexistentCompatibleVersionReturnsNotFound() {
	env := s.setupEnv()
	tq := "functional-versioning-compat-not-found"

	res, err := env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
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
	env := s.setupEnv()
	tq := "functional-versioning-persists"

	s.addNewDefaultBuildID(env, tq, s.prefixed("foo"))

	// Unload task queue to make sure the data is there when we load it again.
	s.unloadTaskQueue(env, tq)

	res, err := env.FrontendClient().GetWorkerBuildIdCompatibility(s.Context(), &workflowservice.GetWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res)
	s.Equal(s.prefixed("foo"), getCurrentDefault(res))
}

func (s *VersioningIntegSuite) TestVersioningChangesPropagate() {
	tq := "functional-versioning-propagate"

	// ensure at least two hops
	const partCount = 1 + partitionTreeDegree + partitionTreeDegree*partitionTreeDegree
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, partCount),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, partCount),
	)

	for _, buildID := range []string{"foo", "foo-v2", "foo-v3"} {
		s.addNewDefaultBuildID(env, tq, buildID)
		s.waitForVersionSetPropagation(env, tq, buildID)
	}
}

func (s *VersioningIntegSuite) TestMaxTaskQueuesPerBuildIdEnforced() {
	env := s.setupEnv()
	buildID := fmt.Sprintf("b-%s", s.T().Name())
	// Map a 3 task queues to this build ID and verify success
	for i := 1; i <= 3; i++ {
		taskQueue := fmt.Sprintf("q-%s-%d", s.T().Name(), i)
		_, err := env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: taskQueue,
			Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
				AddNewBuildIdInNewDefaultSet: buildID,
			},
		})
		s.NoError(err)
	}

	// Map a fourth task queue to this build ID and verify it errors
	taskQueue := fmt.Sprintf("q-%s-%d", s.T().Name(), 4)
	_, err := env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: taskQueue,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: buildID,
		},
	})
	var failedPreconditionError *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionError)
	s.Equal("Exceeded max task queues allowed to be mapped to a single build ID: 3", failedPreconditionError.Message)
}

func (s *VersioningIntegSuite) TestDispatchNewWorkflowOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchNewWorkflow(env, false) })
}

func (s *VersioningIntegSuite) TestDispatchNewWorkflow() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchNewWorkflow(env, true) })
}

func (s *VersioningIntegSuite) dispatchNewWorkflow(
	env *testcore.TestEnv,
	newVersioning bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v1)
		s.waitForAssignmentRulePropagation(env, tq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v1)
		s.waitForVersionSetPropagation(env, tq, v1)
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)

	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, newVersioning, v1, "", nil)
	if newVersioning {
		s.validateWorkflowEventsVersionStamps(env, run.GetID(), run.GetRunID(), []string{v1}, "")
	} else {
		s.validateWorkflowEventsVersionStamps(env, run.GetID(), run.GetRunID(), []string{}, "")
	}
}

func (s *VersioningIntegSuite) TestDispatchNewWorkflowWithRamp() {
	env := s.setupEnv()
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	wf1 := func(ctx workflow.Context) (string, error) {
		return "done v1!", nil
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		return "done v2!", nil
	}

	rule := s.addAssignmentRule(env, tq, v1)
	rule2 := s.addAssignmentRuleWithRamp(env, tq, v2, 50)
	s.waitForAssignmentRulePropagation(env, tq, rule)
	s.waitForAssignmentRulePropagation(env, tq, rule2)

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()
	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	counter := make(map[string]int)
	for range 50 {
		run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
		s.NoError(err)
		var out string
		s.NoError(run.Get(s.Context(), &out))
		counter[out]++
	}

	// both builds should've got executions
	s.Positive(counter["done v1!"])
	s.Positive(counter["done v2!"])
	s.Equal(50, counter["done v1!"]+counter["done v2!"])
}

func (s *VersioningIntegSuite) TestWorkflowStaysInBuildId() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.workflowStaysInBuildID(env) })
}

func (s *VersioningIntegSuite) workflowStaysInBuildID(env *testcore.TestEnv) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	act1Done := make(chan struct{})
	rulesUpdated := make(chan struct{})

	act1 := func() (string, error) {
		close(act1Done)
		return "act1 done!", nil
	}

	act2 := func() (string, error) {
		env.WaitForChannel(rulesUpdated)
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

	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflow(wf)
	w1.RegisterActivity(act1)
	w1.RegisterActivity(act2)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)

	env.WaitForChannel(act1Done)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, true, v1, "", nil)

	// update rules with v2 as the default build
	rule = s.addAssignmentRule(env, tq, v2)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	dw, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
	s.NoError(err)
	s.Len(dw.GetPendingActivities(), 1)
	s.NotNil(dw.GetPendingActivities()[0].GetUseWorkflowBuildId())

	close(rulesUpdated)
	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, true, v1, "", nil)
	s.validateWorkflowEventsVersionStamps(env, run.GetID(), run.GetRunID(), []string{
		v1, // WFT
		v1, // activity
		// v1, skipped because it belongs to sticky queue
		v1, // activity
		// v1, skipped because it belongs to sticky queue
	}, "")
}

func (s *VersioningIntegSuite) TestUnversionedWorkflowStaysUnversioned() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.unversionedWorkflowStaysUnversioned(env) })
}

func (s *VersioningIntegSuite) unversionedWorkflowStaysUnversioned(env *testcore.TestEnv) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	act1Done := make(chan struct{})
	rulesUpdated := make(chan struct{})

	act1 := func() (string, error) {
		close(act1Done)
		return "act1 done!", nil
	}

	act2 := func() (string, error) {
		env.WaitForChannel(rulesUpdated)
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

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflow(wf)
	w1.RegisterActivity(act1)
	w1.RegisterActivity(act2)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)

	env.WaitForChannel(act1Done)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), "", true, "binary-checksum", "", nil)

	// update rules with v1 as the default build
	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	dw, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
	s.NoError(err)
	s.Len(dw.GetPendingActivities(), 1)
	s.Nil(dw.GetPendingActivities()[0].GetAssignedBuildId())
	close(rulesUpdated)

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), "", true, "binary-checksum", "", nil)
	s.validateWorkflowEventsVersionStamps(env, run.GetID(), run.GetRunID(), []string{"", "", "", "", ""}, "")
}

func (s *VersioningIntegSuite) TestFirstWorkflowTaskAssignment_Spooled() {
	s.T().Skip("Skipping test since rules based versioning is soon to be deprecated")
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.firstWorkflowTaskAssignmentSpooled(env) })
}

func (s *VersioningIntegSuite) firstWorkflowTaskAssignmentSpooled(env *testcore.TestEnv) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	v3 := s.prefixed("v3")

	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	// start wf without worker
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		// setting to a short timeout to verify first wf task timeout assignment
		WorkflowTaskTimeout: 1 * time.Second,
	}, "wf")
	s.NoError(err)

	// MS should have the correct build ID
	s.waitForWorkflowBuildID(env, run.GetID(), run.GetRunID(), v1)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, true, "", "", nil)

	// update latest build to v2
	rule = s.addAssignmentRule(env, tq, v2)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	failedTask := make(chan struct{})
	wf1 := func(ctx workflow.Context) (string, error) {
		close(failedTask)
		panic("failing WF task intentionally")
	}

	// run worker on v1 so it can fail the scheduled task
	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	env.WaitForChannel(failedTask)

	// After scheduling the second time, now MS should be assigned to v2
	s.waitForWorkflowBuildID(env, run.GetID(), run.GetRunID(), v2)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v2, true, "", "", []string{v1})

	// update latest build to v3
	rule = s.addAssignmentRule(env, tq, v3)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	timedoutTask := make(chan struct{})
	wf2 := func(ctx workflow.Context) (string, error) {
		time.Sleep(1 * time.Second) //nolint:forbidigo
		timedoutTask <- struct{}{}
		time.Sleep(100 * time.Second) //nolint:forbidigo
		return "return after long sleep", nil
	}

	// run worker on v2 so it can timeout the scheduled task
	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		// since the WF cache is shared by all workers in this process, we need to set this in a way that ensures
		// WFTs will timeout in w2 but not in w3
		DeadlockDetectionTimeout: 1500 * time.Millisecond,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	env.WaitForChannel(timedoutTask)

	// After scheduling the third time, now MS should be assigned to v3
	s.waitForWorkflowBuildID(env, run.GetID(), run.GetRunID(), v3)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v3, true, "", "", []string{v1, v2})

	wf3 := func(ctx workflow.Context) (string, error) {
		return "done on v3!", nil
	}

	// run worker on v3 so it can complete the wf
	w3 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v3,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w3.RegisterWorkflowWithOptions(wf3, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w3.Start())
	defer w3.Stop()

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done on v3!", out)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v3, true, v3, "", []string{v1, v2})
	s.validateWorkflowEventsVersionStamps(env,
		run.GetID(), run.GetRunID(), []string{
			v1, // failed wf tasks
			v2, // timed out wf tasks
			v3, // succeeded wf task
		}, "",
	)
}

func (s *VersioningIntegSuite) TestFirstWorkflowTaskAssignment_SyncMatch() {
	s.T().Skip("Skipping test since rules based versioning is soon to be deprecated")
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.firstWorkflowTaskAssignmentSyncMatch(env) })
}

func (s *VersioningIntegSuite) firstWorkflowTaskAssignmentSyncMatch(env *testcore.TestEnv) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	v3 := s.prefixed("v3")

	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	// v1 fails the task
	failedTask := make(chan struct{})
	wf1 := func(ctx workflow.Context) (string, error) {
		failedTask <- struct{}{}
		panic("failing WF task intentionally")
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		// setting to a short timeout to verify first wf task timeout assignment
		WorkflowTaskTimeout: 1 * time.Second,
	}, "wf")
	s.NoError(err)

	// wait for two failures to make sure more attempts does not generate more history tasks
	env.WaitForChannel(failedTask)
	env.WaitForChannel(failedTask)

	// MS should have the correct build ID
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, true, "", "", nil)

	// v2 times out the task
	timedoutTask := make(chan struct{})
	wf2 := func(ctx workflow.Context) (string, error) {
		time.Sleep(1 * time.Second) //nolint:forbidigo
		timedoutTask <- struct{}{}
		time.Sleep(100 * time.Second) //nolint:forbidigo
		return "return after long sleep", nil
	}

	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		// since the WF cache is shared by all workers in this process, we need to set this in a way that ensures
		// WFTs will timeout in w2 but not in w3
		DeadlockDetectionTimeout: 1500 * time.Millisecond,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// update latest build to v2
	rule = s.addAssignmentRule(env, tq, v2)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	// wait for multiple timeouts to make sure more attempts do not generate more history events
	env.WaitForChannel(timedoutTask)
	env.WaitForChannel(timedoutTask)
	env.WaitForChannel(timedoutTask)

	// After scheduling the second time, now MS should be assigned to v2
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v2, true, "", "", []string{v1})

	// v3 can process the task
	wf3 := func(ctx workflow.Context) (string, error) {
		return "done on v3!", nil
	}

	// run worker on v3 so it can complete the wf
	w3 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v3,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w3.RegisterWorkflowWithOptions(wf3, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w3.Start())
	defer w3.Stop()

	// update latest build to v3
	rule = s.addAssignmentRule(env, tq, v3)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done on v3!", out)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v3, true, v3, "", []string{v1, v2})
	s.validateWorkflowEventsVersionStamps(env,
		run.GetID(), run.GetRunID(), []string{
			v1, // failed wf tasks
			v2, // timed out wf tasks
			v3, // succeeded wf task
		}, "",
	)
}

func (s *VersioningIntegSuite) TestIndependentActivityTaskAssignment_Spooled_VersionedWorkflow() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.independentActivityTaskAssignmentSpooled(env, true)
	})
}

func (s *VersioningIntegSuite) TestIndependentActivityTaskAssignment_Spooled_UnversionedWorkflow() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.independentActivityTaskAssignmentSpooled(env, false)
	})
}

func (s *VersioningIntegSuite) independentActivityTaskAssignmentSpooled(
	env *testcore.TestEnv,
	versionedWf bool,
) {
	wfTq := testcore.RandomizeStr(s.T().Name())
	actTq := testcore.RandomizeStr(s.T().Name())
	wfV1 := s.prefixed("wfv1")
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	v3 := s.prefixed("v3")

	rule := s.addAssignmentRule(env, actTq, v1)
	s.waitForAssignmentRulePropagation(env, actTq, rule)
	if versionedWf {
		rule = s.addAssignmentRule(env, wfTq, wfV1)
		s.waitForAssignmentRulePropagation(env, wfTq, rule)
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
	wfw := worker.New(env.SdkClient(), wfTq, worker.Options{
		BuildID:                          wfV1,
		UseBuildIDForVersioning:          versionedWf,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	wfw.RegisterWorkflow(wf)
	s.NoError(wfw.Start())
	defer wfw.Stop()

	// start wf without activity worker
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue: wfTq,
		// setting to a short timeout to verify first wf task timeout assignment
		WorkflowTaskTimeout: 1 * time.Second,
	}, wf)
	s.NoError(err)

	// MS should have the correct build ID after finishing the first WFT
	s.Eventually(
		func() bool {
			dw, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
			s.NoError(err)
			if len(dw.GetPendingActivities()) == 0 {
				return false
			}
			if versionedWf {
				s.Equal(wfV1, dw.GetWorkflowExecutionInfo().GetAssignedBuildId())
				s.Equal(wfV1, dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId())
			} else {
				s.Empty(dw.GetWorkflowExecutionInfo().GetAssignedBuildId()) //nolint:staticcheck
				s.False(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetUseVersioning())
			}
			return v1 == dw.GetPendingActivities()[0].GetLastIndependentlyAssignedBuildId()
		},
		10*time.Second,
		50*time.Millisecond,
	)

	// update latest build to v2
	rule = s.addAssignmentRule(env, actTq, v2)
	s.waitForAssignmentRulePropagation(env, actTq, rule)

	failedTask := make(chan struct{})
	act1 := func() (string, error) {
		close(failedTask)
		return "", errors.New("failing activity task intentionally") //nolint:err113
	}

	// run v1 activity worker so it can fail the scheduled activity
	w1 := worker.New(env.SdkClient(), actTq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	env.WaitForChannel(failedTask)

	// After scheduling the second time, now pending activity should be assigned to v2
	s.Eventually(
		func() bool {
			dw, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
			s.NoError(err)
			s.Len(dw.GetPendingActivities(), 1)
			return v2 == dw.GetPendingActivities()[0].GetLastIndependentlyAssignedBuildId()
		},
		10*time.Second,
		50*time.Millisecond,
	)

	// update latest build to v3
	rule = s.addAssignmentRule(env, actTq, v3)
	s.waitForAssignmentRulePropagation(env, actTq, rule)

	timedoutTask := make(chan struct{})
	act2 := func() (string, error) {
		time.Sleep(1 * time.Second) //nolint:forbidigo
		close(timedoutTask)
		time.Sleep(100 * time.Second) //nolint:forbidigo
		return "return after long sleep", nil
	}

	// run v2 activity worker so it can timeout the scheduled activity
	w2 := worker.New(env.SdkClient(), actTq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		// since the WF cache is shared by all workers in this process, we need to set this in a way that ensures
		// WFTs will timeout in w2 but not in w3
		DeadlockDetectionTimeout: 1500 * time.Millisecond,
	})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act"})
	s.NoError(w2.Start())
	defer w2.Stop()

	env.WaitForChannel(timedoutTask)

	// After scheduling the third time, now pending activity should be assigned to v3
	s.Eventually(
		func() bool {
			dw, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
			s.NoError(err)
			s.Len(dw.GetPendingActivities(), 1)
			return v3 == dw.GetPendingActivities()[0].GetLastIndependentlyAssignedBuildId()
		},
		10*time.Second,
		50*time.Millisecond,
	)

	act3 := func() (string, error) {
		return "done in v3!", nil
	}

	// run worker on v3 so it can complete the wf
	w3 := worker.New(env.SdkClient(), actTq, worker.Options{
		BuildID:                          v3,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w3.RegisterActivityWithOptions(act3, activity.RegisterOptions{Name: "act"})
	s.NoError(w3.Start())
	defer w3.Stop()

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)

	if versionedWf {
		s.validateWorkflowEventsVersionStamps(env,
			run.GetID(), run.GetRunID(), []string{
				wfV1,
				v3, // succeeded activity
				// wfV1, removed because it's on a sticky queue
			}, "",
		)
	} else {
		s.validateWorkflowEventsVersionStamps(env,
			run.GetID(), run.GetRunID(), []string{
				"",
				v3, // succeeded activity
				"",
			}, "",
		)
	}
}

func (s *VersioningIntegSuite) TestIndependentActivityTaskAssignment_SyncMatch_VersionedWorkflow() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.independentActivityTaskAssignmentSyncMatch(env, true)
	})
}

func (s *VersioningIntegSuite) TestIndependentActivityTaskAssignment_SyncMatch_UnversionedWorkflow() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.independentActivityTaskAssignmentSyncMatch(env, false)
	})
}

func (s *VersioningIntegSuite) independentActivityTaskAssignmentSyncMatch(
	env *testcore.TestEnv,
	versionedWf bool,
) {
	wfTq := testcore.RandomizeStr(s.T().Name())
	actTq := testcore.RandomizeStr(s.T().Name())
	wfV1 := s.prefixed("wfv1")
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	v3 := s.prefixed("v3")

	rule := s.addAssignmentRule(env, actTq, v1)
	s.waitForAssignmentRulePropagation(env, actTq, rule)
	if versionedWf {
		rule := s.addAssignmentRule(env, wfTq, wfV1)
		s.waitForAssignmentRulePropagation(env, wfTq, rule)
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
	wfw := worker.New(env.SdkClient(), wfTq, worker.Options{
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

	w1 := worker.New(env.SdkClient(), actTq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue: wfTq,
		// setting to a short timeout to verify first wf task timeout assignment
		WorkflowTaskTimeout: 1 * time.Second,
	}, wf)
	s.NoError(err)

	env.WaitForChannel(failedTask)

	// MS should have the correct build ID after finishing the first WFT
	s.Eventually(
		func() bool {
			dw, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
			s.NoError(err)
			if len(dw.GetPendingActivities()) == 0 {
				return false
			}
			if versionedWf {
				s.Equal(wfV1, dw.GetWorkflowExecutionInfo().GetAssignedBuildId())
				s.Equal(wfV1, dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId())
			} else {
				s.Empty(dw.GetWorkflowExecutionInfo().GetAssignedBuildId()) //nolint:staticcheck
				s.False(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetUseVersioning())
			}
			return v1 == dw.GetPendingActivities()[0].GetLastIndependentlyAssignedBuildId()
		},
		10*time.Second,
		50*time.Millisecond,
	)

	// v2 timesout the activity
	timedoutTask := make(chan struct{})
	act2 := func() (string, error) {
		time.Sleep(1 * time.Second) //nolint:forbidigo
		close(timedoutTask)
		time.Sleep(100 * time.Second) //nolint:forbidigo
		return "return after long sleep", nil
	}

	w2 := worker.New(env.SdkClient(), actTq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		// since the WF cache is shared by all workers in this process, we need to set this in a way that ensures
		// WFTs will timeout in w2 but not in w3
		DeadlockDetectionTimeout: 1500 * time.Millisecond,
	})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// update latest build to v2
	rule = s.addAssignmentRule(env, actTq, v2)
	s.waitForAssignmentRulePropagation(env, actTq, rule)

	env.WaitForChannel(timedoutTask)

	// After scheduling the second time, now pending activity should be assigned to v2
	s.Eventually(
		func() bool {
			dw, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
			s.NoError(err)
			s.Len(dw.GetPendingActivities(), 1)
			return v2 == dw.GetPendingActivities()[0].GetLastIndependentlyAssignedBuildId()
		},
		10*time.Second,
		50*time.Millisecond,
	)

	// v3 can process the activity
	act3 := func() (string, error) {
		return "done in v3!", nil
	}

	// run worker on v3 so it can complete the wf
	w3 := worker.New(env.SdkClient(), actTq, worker.Options{
		BuildID:                          v3,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w3.RegisterActivityWithOptions(act3, activity.RegisterOptions{Name: "act"})
	s.NoError(w3.Start())
	defer w3.Stop()

	// update latest build to v3
	rule = s.addAssignmentRule(env, actTq, v3)
	s.waitForAssignmentRulePropagation(env, actTq, rule)

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)

	if versionedWf {
		s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), wfV1, true, wfV1, "", nil)
		s.validateWorkflowEventsVersionStamps(env,
			run.GetID(), run.GetRunID(), []string{
				wfV1,
				v3, // succeeded activity
				// wfV1, skipping stamp because this is a sticky queue task
			}, "",
		)
	} else {
		s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), "", true, wfV1, "", nil)
		s.validateWorkflowEventsVersionStamps(env,
			run.GetID(), run.GetRunID(), []string{
				"",
				v3, // succeeded activity
				"",
			}, "",
		)
	}
}

func (s *VersioningIntegSuite) TestWorkflowTaskRedirectInRetryFirstTask() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.testWorkflowTaskRedirectInRetry(env, true)
	})
}

func (s *VersioningIntegSuite) TestWorkflowTaskRedirectInRetryNonFirstTask() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.testWorkflowTaskRedirectInRetry(env, false)
	})
}

func (s *VersioningIntegSuite) testWorkflowTaskRedirectInRetry(
	env *testcore.TestEnv,
	firstTask bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v1.1")
	v12 := s.prefixed("v1.2")

	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	act := func() error {
		return nil
	}

	// v1 fails the task
	failedTask := make(chan struct{})
	wf1 := func(ctx workflow.Context) (string, error) {
		if !firstTask {
			// add an activity to move workflow past first WFT
			var out any
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 1 * time.Second}), act).Get(ctx, &out)
			s.NoError(err)
		}
		failedTask <- struct{}{}
		panic("failing WF task intentionally")
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivity(act)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
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
	env.WaitForChannel(failedTask)
	env.WaitForChannel(failedTask)
	env.WaitForChannel(failedTask)

	expectedStampBuildId := ""
	if !firstTask {
		expectedStampBuildId = v1
	}
	// MS should have the correct build ID
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, true, expectedStampBuildId, "", nil)

	// v11 times out the task
	timedoutTask := make(chan struct{})
	wf11 := func(ctx workflow.Context) (string, error) {
		if !firstTask {
			// add an activity to move workflow past first WFT
			var out any
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 1 * time.Second}), act).Get(ctx, &out)
			s.NoError(err)
		}
		time.Sleep(1 * time.Second) //nolint:forbidigo
		timedoutTask <- struct{}{}
		time.Sleep(100 * time.Second) //nolint:forbidigo
		return "return after long sleep", nil
	}

	w11 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		// since the WF cache is shared by all workers in this process, we need to set this in a way that ensures
		// WFTs will timeout in w2 but not in w3
		DeadlockDetectionTimeout: 1500 * time.Millisecond,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	w11.RegisterActivity(act)
	s.NoError(w11.Start())
	defer w11.Stop()

	rule2 := s.addRedirectRule(env, tq, v1, v11)
	s.waitForRedirectRulePropagation(env, tq, rule2)

	// wait for multiple timeouts to make sure more attempts does not generate more history tasks
	env.WaitForChannel(timedoutTask)
	env.WaitForChannel(timedoutTask)
	env.WaitForChannel(timedoutTask)
	// After scheduling the second time, now MS should be assigned to v2
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v11, true, expectedStampBuildId, "", []string{v1})

	// v12 can process the task
	wf12 := func(ctx workflow.Context) (string, error) {
		if !firstTask {
			// add an activity to move workflow past first WFT
			var out any
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 1 * time.Second}), act).Get(ctx, &out)
			s.NoError(err)
		}
		return "done on v1.2!", nil
	}

	// run worker on v12 so it can complete the wf
	w12 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v12,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w12.RegisterWorkflowWithOptions(wf12, workflow.RegisterOptions{Name: "wf"})
	w12.RegisterActivity(act)
	s.NoError(w12.Start())
	defer w12.Stop()

	rule2 = s.addRedirectRule(env, tq, v11, v12)
	s.waitForRedirectRulePropagation(env, tq, rule2)

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done on v1.2!", out)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v12, true, v12, "", []string{v1, v11})
	expectedStamps := []string{
		v1,  // failed wf task
		v11, // timed out wf task show up in history because a redirect happened after them
		v12, // succeeded wf task
	}
	if !firstTask {
		expectedStamps = []string{
			v1, // first wf task
			v1, // activity task
			// v1,  // skipping stamp for failed wf task on sticky queue
			v1,  // failed wf task on normal queue
			v11, // timed out wf task show up in history because they happened on a different build ID
			v12, // succeeded wf task
		}
	}
	s.validateWorkflowEventsVersionStamps(env, run.GetID(), run.GetRunID(), expectedStamps, "")
}

func (s *VersioningIntegSuite) TestDispatchNotUsingVersioningOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchNotUsingVersioning(env, false) })
}

func (s *VersioningIntegSuite) TestDispatchNotUsingVersioning() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchNotUsingVersioning(env, true) })
}

func (s *VersioningIntegSuite) dispatchNotUsingVersioning(
	env *testcore.TestEnv,
	newVersioning bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	wf1nover := func(ctx workflow.Context) (string, error) {
		return "done without versioning!", nil
	}
	wf1 := func(ctx workflow.Context) (string, error) {
		return "done with versioning!", nil
	}

	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v1)
		s.waitForAssignmentRulePropagation(env, tq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v1)
		s.waitForVersionSetPropagation(env, tq, v1)
	}

	w1nover := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          false,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1 := worker.New(env.SdkClient(), tq, worker.Options{
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

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done with versioning!", out)

	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, newVersioning, v1, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchNewWorkflowStartWorkerFirst() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchNewWorkflowStartWorkerFirst(env) })
}

func (s *VersioningIntegSuite) dispatchNewWorkflowStartWorkerFirst(env *testcore.TestEnv) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	// run worker before registering build. it will use guessed set id
	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	// wait for it to start polling
	time.Sleep(200 * time.Millisecond) //nolint:forbidigo

	s.addNewDefaultBuildID(env, tq, v1)
	s.waitForVersionSetPropagation(env, tq, v1)

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)

	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, false, v1, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchUnversionedRemainsUnversioned() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.dispatchUnversionedRemainsUnversioned(env)
	})
}

func (s *VersioningIntegSuite) dispatchUnversionedRemainsUnversioned(env *testcore.TestEnv) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	started := make(chan struct{}, 1)

	wf := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		return "done!", nil
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		// no build ID
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)

	env.WaitForChannel(started)
	s.addNewDefaultBuildID(env, tq, v1)
	s.waitForVersionSetPropagation(env, tq, v1)

	// unblock the workflow
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)

	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), "", false, "binary-checksum", "", nil)
}

func (s *VersioningIntegSuite) TestDispatchUpgradeStopOldOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchUpgrade(env, false, true) })
}

func (s *VersioningIntegSuite) TestDispatchUpgradeWaitOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchUpgrade(env, false, false) })
}

func (s *VersioningIntegSuite) TestDispatchUpgradeStopOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchUpgrade(env, true, true) })
}

func (s *VersioningIntegSuite) TestDispatchUpgradeWait() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchUpgrade(env, true, false) })
}

func (s *VersioningIntegSuite) dispatchUpgrade(
	env *testcore.TestEnv,
	newVersioning, stopOld bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
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

	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v1)
		s.waitForAssignmentRulePropagation(env, tq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v1)
		s.waitForVersionSetPropagation(env, tq, v1)
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	env.WaitForChannel(started)

	// now add v11 as compatible so the next workflow task runs there
	if newVersioning {
		rule := s.addRedirectRule(env, tq, v1, v11)
		s.waitForRedirectRulePropagation(env, tq, rule)
	} else {
		s.addCompatibleBuildID(env, tq, v11, v1, false)
		s.waitForVersionSetPropagation(env, tq, v11)
	}
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	w11 := worker.New(env.SdkClient(), tq, worker.Options{
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
		// Don't stop the old worker.
		if !newVersioning {
			// In this case, w1 will still have some pollers blocked on
			// the normal queue which could pick up tasks that we want to go to v11. (We don't
			// interrupt long polls.) To ensure those polls don't interfere, wait for them to
			// expire.
			time.Sleep(longPollTime) //nolint:forbidigo
		}
	}

	// unblock the workflow
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done from 1.1!", out)

	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v11, newVersioning, v11, "", []string{v1})
}

type activityFailMode int

const (
	dontFailActivity = iota
	failActivity
	timeoutActivity
)

func (s *VersioningIntegSuite) TestDispatchActivityOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.dispatchActivity(env, dontFailActivity, false, false)
	})
}

func (s *VersioningIntegSuite) TestDispatchActivityFailOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.dispatchActivity(env, failActivity, false, false)
	})
}

func (s *VersioningIntegSuite) TestDispatchActivityTimeoutOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.dispatchActivity(env, timeoutActivity, false, false)
	})
}

func (s *VersioningIntegSuite) TestDispatchActivity() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.dispatchActivity(env, dontFailActivity, true, false)
	})
}

func (s *VersioningIntegSuite) TestDispatchActivityFail() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.dispatchActivity(env, failActivity, true, false)
	})
}

func (s *VersioningIntegSuite) TestDispatchActivityTimeout() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.dispatchActivity(env, timeoutActivity, true, false)
	})
}

func (s *VersioningIntegSuite) TestDispatchActivityCrossTq() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.dispatchActivity(env, dontFailActivity, true, true)
	})
}

func (s *VersioningIntegSuite) TestDispatchActivityFailCrossTq() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.dispatchActivity(env, failActivity, true, true)
	})
}

func (s *VersioningIntegSuite) TestDispatchActivityTimeoutCrossTq() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) {
		s.dispatchActivity(env, timeoutActivity, true, true)
	})
}

func (s *VersioningIntegSuite) dispatchActivity(
	env *testcore.TestEnv,
	failMode activityFailMode, newVersioning bool, crossTq bool,
) {
	// This also implicitly tests that a workflow stays on a compatible version set if a new
	// incompatible set is registered, because wf2 just panics. It further tests that
	// stickiness on v1 is not broken by registering v2, because the channel send will panic on
	// replay after we close the channel.

	tq := testcore.RandomizeStr(s.T().Name())
	actxTq := testcore.RandomizeStr(s.T().Name() + "activity")
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
				time.Sleep(5 * time.Second) //nolint:forbidigo
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

	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v1)
		s.waitForAssignmentRulePropagation(env, tq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v1)
		s.waitForVersionSetPropagation(env, tq, v1)
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()
	w1xTq := worker.New(env.SdkClient(), actxTq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1xTq.RegisterActivityWithOptions(act1xTq, activity.RegisterOptions{Name: "act"})
	s.NoError(w1xTq.Start())
	defer w1xTq.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	env.WaitForChannel(started)
	close(started) // force panic if replayed

	// now register v2 as default
	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v2)
		s.waitForAssignmentRulePropagation(env, tq, rule)
		rule = s.addAssignmentRule(env, actxTq, v2)
		s.waitForAssignmentRulePropagation(env, actxTq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v2)
		s.waitForVersionSetPropagation(env, tq, v2)
	}

	// start worker for v2
	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterActivityWithOptions(act2, activity.RegisterOptions{Name: "act"})
	s.NoError(w2.Start())
	defer w2.Stop()
	w2xTq := worker.New(env.SdkClient(), actxTq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2xTq.RegisterActivityWithOptions(act2xTq, activity.RegisterOptions{Name: "act"})
	s.NoError(w2xTq.Start())
	defer w2xTq.Stop()

	// unblock the workflow
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(s.Context(), &out))
	if crossTq {
		s.Equal("v1xTqv2xTq", out)
	} else {
		s.Equal("v1v2", out)
	}

	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, newVersioning, v1, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchActivityUpgrade() {
	env := s.setupEnv()
	// Testing:
	// - redirecting dependent activity redirects wf
	//   - this also implicitly verifies that sticky queue is cleared after redirecting activity task
	// - redirecting independent activity does not redirect wf

	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v1.1")
	v12 := s.prefixed("v1.2")

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
		env.WaitForChannel(proceed11)
		return "v1.1", nil
	}
	act12 := func() (string, error) {
		started12 <- struct{}{}
		env.WaitForChannel(proceed12)
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

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		DeadlockDetectionTimeout:         5 * time.Second,
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	w11 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		DeadlockDetectionTimeout:         5 * time.Second,
	})
	w11.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	w11.RegisterActivityWithOptions(act11, activity.RegisterOptions{Name: "act"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w12 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v12,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
		DeadlockDetectionTimeout:         5 * time.Second,
	})
	w12.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	w12.RegisterActivityWithOptions(act12, activity.RegisterOptions{Name: "act"})
	s.NoError(w12.Start())
	defer w12.Stop()

	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	// wait for it to start on v1
	env.WaitForChannel(startedWf)
	rule2 := s.addRedirectRule(env, tq, v1, v11)
	s.waitForRedirectRulePropagation(env, tq, rule2)
	env.SendToChannel(proceedWf)

	env.WaitForChannel(started11)
	// wf assigned build ID should be updated by activity redirect
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v11, true, v1, "", []string{v1})
	// let activity finish
	env.SendToChannel(proceed11)

	// wf replays on 1.1 so need to unblock it an extra time
	env.WaitForChannel(startedWf)
	env.SendToChannel(proceedWf)

	env.WaitForChannel(startedWf)
	rule2 = s.addRedirectRule(env, tq, v11, v12)
	s.waitForRedirectRulePropagation(env, tq, rule2)
	env.SendToChannel(proceedWf)

	env.WaitForChannel(started12)
	// wf assigned build ID should not be updated by independent activity redirect
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v11, true, v11, "", []string{v1})
	// let activity finish
	env.SendToChannel(proceed12)

	// wf replays on 1.2 so need to unblock it two extra times
	env.WaitForChannel(startedWf)
	env.SendToChannel(proceedWf)
	env.WaitForChannel(startedWf)
	env.SendToChannel(proceedWf)

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("v1.1v1.2", out)

	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v12, true, v12, "", []string{v1, v11})
}

func (s *VersioningIntegSuite) TestRedirectWithConcurrentActivities() {
	s.T().Skip("Skipping test since this tests old versioning behavior and also flakes")
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingGetUserDataLongPollTimeout, 2*time.Second),
	)
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

	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1.0")

	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

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

	act := func(version string, runID int32) (string, error) {
		runs := activityCounter.Add(1)
		s.T().Logf("Starting activity %d on %s at %d\n", runID, version, runs)
		if lastRedirectTarget.CompareAndSwap(version, version+" observed") && version != versions[0] {
			// The last redirect rule is applied and observed by an activity, now delete it to make sure wf keeps using
			// the right build ID after applying the redirect rule, even when the rule is not present anymore.
			index, err := strconv.Atoi(version[len(version)-1:]) // get the last char of version is the index in the versions array
			s.NoError(err)
			s.T().Logf("Removing redirect from %s to %s \n", versions[index-1], version)
			s.removeRedirectRule(env, tq, versions[index-1])
			lastRedirectTarget.CompareAndSwap(version+" observed", version+" redirect cleaned")
		}
		if rand.Float64() < activityErrorRate {
			return "", errors.New("intentionally failing activity")
		}
		if triggerRedirectAtActivityRun.Load() == runID {
			// When enough activities are run using the current version, add redirect rule to the next version.
			v := runID / activityRuns
			if int(v+1) < len(versions) {
				// wait for last redirect rule to be cleaned up
				for !lastRedirectTarget.CompareAndSwap(versions[v]+" redirect cleaned", versions[v+1]) {
				}
				s.T().Logf("Adding redirect from %s to %s at %d\n", versions[v], versions[v+1], runs)
				s.addRedirectRule(env, tq, versions[v], versions[v+1])
				// Intentionally do not wait for propagation of the rules to partitions. Waiting will linger this
				// activity and allows all the other concurrent activities to finish, leaving only the WFT task to
				// see the redirect rule for the first time.
				triggerRedirectAtActivityRun.CompareAndSwap(runID, runID+activityRuns)
			}
		}

		// Add random sleep to simulate network delay
		//nolint:forbidigo
		time.Sleep(time.Duration(int64(rand.Intn(50)) * int64(time.Millisecond))) //nolint:forbidigo
		s.T().Logf("Completing activity %d on %s at %d\n", runID, version, runs)
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
				s.GreaterOrEqual(wfVersion, activityVersion)
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
		w := worker.New(env.SdkClient(), tq, worker.Options{
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
			func(runID int32) (string, error) {
				return act(v, runID)
			},
			activity.RegisterOptions{Name: "act"})
		s.NoError(w.Start())
		workers = append(workers, w)
	}

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	// Workflow should finish, otherwise it may mean we dropped some task without rescheduling them in the new build ID
	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), versions[9], true, versions[9], "", versions[:9])

	activityPerVersion := make(map[string]int)
	for _, v := range strings.Split(out, " ") {
		activityPerVersion[v]++
	}

	wh := env.SdkClient().GetWorkflowHistory(s.Context(), run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	redirectAppliedToActivityTask := false
	activityRetried := false
	sawUnorderedEvents := false
	var maxBuildId string
	var maxStartedTimestamp time.Time
	for wh.HasNext() {
		he, err := wh.Next()
		s.NoError(err)
		var taskStartedStamp *commonpb.WorkerVersionStamp
		var taskRedirectCounter int64
		var buildID string
		if activityStarted := he.GetActivityTaskStartedEventAttributes(); activityStarted != nil {
			taskStartedStamp = activityStarted.GetWorkerVersion()
			buildID = taskStartedStamp.GetBuildId()
			if buildID > maxBuildId {
				redirectAppliedToActivityTask = true
			}
			if activityStarted.Attempt > 1 {
				activityRetried = true
			}
			s.True(taskStartedStamp.GetUseVersioning())
			taskRedirectCounter = activityStarted.GetBuildIdRedirectCounter()
			activityPerVersion[buildID]--
		} else if wfStarted := he.GetWorkflowTaskStartedEventAttributes(); wfStarted != nil {
			taskStartedStamp = wfStarted.GetWorkerVersion()
			if taskStartedStamp != nil {
				// taskStartedStamp is nil for sticky queues
				s.True(taskStartedStamp.GetUseVersioning())
				buildID = taskStartedStamp.GetBuildId()
				taskRedirectCounter = wfStarted.GetBuildIdRedirectCounter()
			}
		}
		if he.EventTime.AsTime().Before(maxStartedTimestamp) {
			sawUnorderedEvents = true
		} else {
			maxStartedTimestamp = he.EventTime.AsTime()
		}
		if buildID > maxBuildId {
			maxBuildId = buildID
		}
		if taskStartedStamp != nil {
			// the last char of version is the index in the versions array which is the expected redirect counter for
			// a task started event
			expectedRedirectCounter, err := strconv.Atoi(buildID[len(buildID)-1:])
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
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchActivityCompatible(env) })
}

func (s *VersioningIntegSuite) dispatchActivityCompatible(env *testcore.TestEnv) {
	tq := testcore.RandomizeStr(s.T().Name())
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

	s.addNewDefaultBuildID(env, tq, v1)
	s.waitForVersionSetPropagation(env, tq, v1)

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterActivityWithOptions(act1, activity.RegisterOptions{Name: "act"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	env.WaitForChannel(started)

	// now register v1.1 as compatible
	s.addCompatibleBuildID(env, tq, v11, v1, false)
	s.waitForVersionSetPropagation(env, tq, v11)
	// start worker for v1.1
	w11 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w11.RegisterActivityWithOptions(act11, activity.RegisterOptions{Name: "act"})
	s.NoError(w11.Start())
	defer w11.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime) //nolint:forbidigo

	// unblock the workflow
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("v1.1", out)

	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v11, false, v11, "", []string{v1})
}

func (s *VersioningIntegSuite) TestDispatchActivityEager() {
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.EnableActivityEagerExecution, true),
	)

	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	_, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	pollResponse, err := env.SdkClient().WorkflowService().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: tq},
		Identity:  "test",
		WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			BuildId: v1,
		},
	})
	s.NoError(err)
	startToCloseTimeout := time.Minute

	completionResponse, err := env.SdkClient().WorkflowService().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
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
	s.NoError(err)
	s.Len(completionResponse.ActivityTasks, 1)
	s.Equal("compatible", completionResponse.ActivityTasks[0].ActivityId)
}

func (s *VersioningIntegSuite) TestDispatchActivityCrossTQFails() {
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)

	tq := testcore.RandomizeStr(s.T().Name())
	crosstq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	act := func() (string, error) { return "v1", nil }
	wf := func(ctx workflow.Context) (string, error) {
		fut := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 1 * time.Second,
			TaskQueue:           crosstq,
			VersioningIntent:    temporal.VersioningIntentCompatible,
		}), "act")
		var val string
		if err := fut.Get(ctx, &val); err != nil {
			return "", err
		}
		return val, nil
	}

	s.addNewDefaultBuildID(env, tq, v1)
	s.addNewDefaultBuildID(env, crosstq, v1)
	s.waitForVersionSetPropagation(env, tq, v1)
	s.waitForVersionSetPropagation(env, crosstq, v1)

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	w1cross := worker.New(env.SdkClient(), crosstq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1cross.RegisterActivityWithOptions(act, activity.RegisterOptions{Name: "act"})
	s.NoError(w1cross.Start())
	defer w1cross.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	s.waitForWorkflowTaskFailedCause(env, run, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES)
	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), run.GetID(), run.GetRunID(), "test complete", nil))
}

func (s *VersioningIntegSuite) TestDispatchChildWorkflowOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchChildWorkflow(env, false, false) })
}

func (s *VersioningIntegSuite) TestDispatchChildWorkflow() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchChildWorkflow(env, true, false) })
}

func (s *VersioningIntegSuite) TestDispatchChildWorkflowCrossTq() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchChildWorkflow(env, true, true) })
}

func (s *VersioningIntegSuite) dispatchChildWorkflow(
	env *testcore.TestEnv,
	newVersioning bool, crossTq bool,
) {
	// This also implicitly tests that a workflow stays on a compatible version set if a new
	// incompatible set is registered, because wf2 just panics. It further tests that
	// stickiness on v1 is not broken by registering v2, because the channel send will panic on
	// replay after we close the channel.

	tq := testcore.RandomizeStr(s.T().Name())
	childxTq := testcore.RandomizeStr(s.T().Name() + "child")
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	inheritedBuildId := ""
	if newVersioning {
		inheritedBuildId = v1
	}

	started := make(chan struct{}, 1)

	validateChildBuild := func(cctx workflow.Context, expectedBuildId, expectedInheritedBuildId string) {
		exec := workflow.GetInfo(cctx).WorkflowExecution
		expectedStampBuildId := ""
		if !newVersioning && expectedBuildId == v1 {
			expectedStampBuildId = expectedBuildId
		}
		s.validateWorkflowBuildIds(env,
			exec.ID,
			exec.RunID,
			expectedBuildId,
			newVersioning,
			expectedStampBuildId,
			expectedInheritedBuildId,
			nil,
		)
		if newVersioning {
			s.validateWorkflowEventsVersionStamps(env, exec.ID, exec.RunID, []string{expectedBuildId}, expectedInheritedBuildId)
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
		rule := s.addAssignmentRule(env, tq, v1)
		s.waitForAssignmentRulePropagation(env, tq, rule)
		rule = s.addAssignmentRule(env, childxTq, v1)
		s.waitForAssignmentRulePropagation(env, childxTq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v1)
		s.waitForVersionSetPropagation(env, tq, v1)
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterWorkflowWithOptions(child1, workflow.RegisterOptions{Name: "child"})
	s.NoError(w1.Start())
	defer w1.Stop()
	w1xTq := worker.New(env.SdkClient(), childxTq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1xTq.RegisterWorkflowWithOptions(child1xTq, workflow.RegisterOptions{Name: "child"})
	s.NoError(w1xTq.Start())
	defer w1xTq.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	env.WaitForChannel(started)
	close(started) // force panic if replayed

	// now register v2 as default
	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v2)
		s.waitForAssignmentRulePropagation(env, tq, rule)
		rule = s.addAssignmentRule(env, childxTq, v2)
		s.waitForAssignmentRulePropagation(env, childxTq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v2)
		s.waitForVersionSetPropagation(env, tq, v2)
	}

	// start worker for v2
	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterWorkflowWithOptions(child2, workflow.RegisterOptions{Name: "child"})
	s.NoError(w2.Start())
	defer w2.Stop()
	w2xTq := worker.New(env.SdkClient(), childxTq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2xTq.RegisterWorkflowWithOptions(child2xTq, workflow.RegisterOptions{Name: "child"})
	s.NoError(w2xTq.Start())
	defer w2xTq.Stop()

	// unblock the workflow
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(s.Context(), &out))
	if crossTq {
		s.Equal("v1xTqv2xTq", out)
	} else {
		s.Equal("v1v2", out)
	}

	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, newVersioning, v1, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchChildWorkflowUpgradeOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchChildWorkflowUpgrade(env, false) })
}

func (s *VersioningIntegSuite) TestDispatchChildWorkflowUpgrade() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchChildWorkflowUpgrade(env, true) })
}

func (s *VersioningIntegSuite) dispatchChildWorkflowUpgrade(
	env *testcore.TestEnv,
	newVersioning bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	inheritedBuildId := ""
	if newVersioning {
		inheritedBuildId = v11
	}

	started := make(chan struct{}, 2)

	child1 := func(cctx workflow.Context) (string, error) {
		exec := workflow.GetInfo(cctx).WorkflowExecution
		expectedStampBuildId := v1
		if newVersioning {
			expectedStampBuildId = ""
		}
		s.validateWorkflowBuildIds(env, exec.ID, exec.RunID, v1, newVersioning, expectedStampBuildId, inheritedBuildId, nil)
		return "v1", nil
	}
	child11 := func(cctx workflow.Context) (string, error) {
		exec := workflow.GetInfo(cctx).WorkflowExecution
		expectedStampBuildId := v11
		if newVersioning {
			expectedStampBuildId = ""
		}
		s.validateWorkflowBuildIds(env, exec.ID, exec.RunID, v11, newVersioning, expectedStampBuildId, inheritedBuildId, nil)
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
		rule := s.addAssignmentRule(env, tq, v1)
		s.waitForAssignmentRulePropagation(env, tq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v1)
		s.waitForVersionSetPropagation(env, tq, v1)
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	w1.RegisterWorkflowWithOptions(child1, workflow.RegisterOptions{Name: "child"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	env.WaitForChannel(started)

	// now register v1.1 as compatible
	if newVersioning {
		rule := s.addRedirectRule(env, tq, v1, v11)
		s.waitForRedirectRulePropagation(env, tq, rule)
	} else {
		s.addCompatibleBuildID(env, tq, v11, v1, false)
		s.waitForVersionSetPropagation(env, tq, v11)
	}
	// start worker for v1.1
	w11 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	w11.RegisterWorkflowWithOptions(child11, workflow.RegisterOptions{Name: "child"})
	s.NoError(w11.Start())
	defer w11.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime) //nolint:forbidigo

	// unblock the workflow
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "wait", nil))

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("v1.1", out)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v11, newVersioning, v11, "", []string{v1})
}

func (s *VersioningIntegSuite) TestDispatchChildWorkflowCrossTQFails() {
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)

	tq := testcore.RandomizeStr(s.T().Name())
	crosstq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")

	child := func(ctx workflow.Context) (string, error) { return "v1", nil }
	wf := func(ctx workflow.Context) (string, error) {
		fut := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:        crosstq,
			VersioningIntent: temporal.VersioningIntentCompatible,
		}), "child")
		var val string
		if err := fut.Get(ctx, &val); err != nil {
			return "", err
		}
		return val, nil
	}

	s.addNewDefaultBuildID(env, tq, v1)
	s.addNewDefaultBuildID(env, crosstq, v1)
	s.waitForVersionSetPropagation(env, tq, v1)
	s.waitForVersionSetPropagation(env, crosstq, v1)

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	w1cross := worker.New(env.SdkClient(), crosstq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1cross.RegisterWorkflowWithOptions(child, workflow.RegisterOptions{Name: "child"})
	s.NoError(w1cross.Start())
	defer w1cross.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	s.waitForWorkflowTaskFailedCause(env, run, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_START_CHILD_EXECUTION_ATTRIBUTES)
	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), run.GetID(), run.GetRunID(), "test complete", nil))
}

func (s *VersioningIntegSuite) waitForWorkflowTaskFailedCause(
	env *testcore.TestEnv,
	run sdkclient.WorkflowRun,
	cause enumspb.WorkflowTaskFailedCause,
) {
	s.Await(func(s *VersioningIntegSuite) {
		events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		})
		for _, event := range events {
			if attrs := event.GetWorkflowTaskFailedEventAttributes(); attrs.GetCause() == cause {
				return
			}
		}
		s.Failf("WorkflowTaskFailed event not found", "cause: %v", cause)
	}, 10*time.Second, 500*time.Millisecond)
}

func (s *VersioningIntegSuite) TestDispatchQueryOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchQuery(env, false) })
}

func (s *VersioningIntegSuite) TestDispatchQuery() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchQuery(env, true) })
}

func (s *VersioningIntegSuite) dispatchQuery(
	env *testcore.TestEnv,
	newVersioning bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
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

	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v1)
		s.waitForAssignmentRulePropagation(env, tq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v1)
		s.waitForVersionSetPropagation(env, tq, v1)
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	env.WaitForChannel(started)

	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v2)
		s.waitForAssignmentRulePropagation(env, tq, rule)
		rrule := s.addRedirectRule(env, tq, v1, v11)
		s.waitForRedirectRulePropagation(env, tq, rrule)
	} else {
		// now register v1.1 as compatible
		// now register v11 as newer compatible with v1 AND v2 as a new default
		s.addCompatibleBuildID(env, tq, v11, v1, false)
		s.addNewDefaultBuildID(env, tq, v2)
		s.waitForVersionSetPropagation(env, tq, v2)
	}
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	// start worker for v1.1 and v2
	w11 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()
	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	if !newVersioning {
		// wait for w1 long polls to all time out
		time.Sleep(longPollTime) //nolint:forbidigo
	}

	// query
	val, err := env.SdkClient().QueryWorkflow(s.Context(), run.GetID(), run.GetRunID(), "query")
	s.NoError(err)
	var out string
	s.NoError(val.Get(&out))
	s.Equal("v1.1", out)

	// let the workflow complete
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "wait", nil))

	// wait for completion
	s.NoError(run.Get(s.Context(), nil))

	// query on closed workflow
	val, err = env.SdkClient().QueryWorkflow(s.Context(), run.GetID(), run.GetRunID(), "query")
	s.NoError(err)
	s.NoError(val.Get(&out))
	s.Equal("v1.1", out)

	// start another wf on v2. should complete immediately.
	run2, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)

	// wait for completion
	s.NoError(run2.Get(s.Context(), nil))

	// query on closed workflow
	val, err = env.SdkClient().QueryWorkflow(s.Context(), run2.GetID(), run2.GetRunID(), "query")
	s.NoError(err)
	s.NoError(val.Get(&out))
	s.Equal("v2", out)
}

func (s *VersioningIntegSuite) TestDispatchContinueAsNewOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchContinueAsNew(env, false, false) })
}

func (s *VersioningIntegSuite) TestDispatchContinueAsNew() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchContinueAsNew(env, true, false) })
}

func (s *VersioningIntegSuite) TestDispatchContinueAsNewCrossTq() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchContinueAsNew(env, true, true) })
}

func (s *VersioningIntegSuite) dispatchContinueAsNew(
	env *testcore.TestEnv,
	newVersioning bool, crossTq bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
	canxTq := testcore.RandomizeStr(s.T().Name() + "CaN")
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")
	inheritedBuildId := ""
	if newVersioning {
		inheritedBuildId = v1
	}

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
			s.validateWorkflowBuildIds(env, exec.ID, exec.RunID, v1, newVersioning, expectedStampBuildId, inheritedBuildId, nil)
			if newVersioning {
				s.validateWorkflowEventsVersionStamps(env, exec.ID, exec.RunID, []string{v1}, inheritedBuildId)
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
			s.validateWorkflowBuildIds(env, exec.ID, exec.RunID, v2, newVersioning, "", "", nil)
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
		rule := s.addAssignmentRule(env, tq, v1)
		s.waitForAssignmentRulePropagation(env, tq, rule)
		rule = s.addAssignmentRule(env, canxTq, v1)
		s.waitForAssignmentRulePropagation(env, canxTq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v1)
		s.waitForVersionSetPropagation(env, tq, v1)
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()
	w1xTq := worker.New(env.SdkClient(), canxTq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1xTq.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1xTq.Start())
	defer w1xTq.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	env.WaitForChannel(started1)

	// now make v2 as a new default
	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v2)
		s.waitForAssignmentRulePropagation(env, tq, rule)
		rule = s.addAssignmentRule(env, canxTq, v2)
		s.waitForAssignmentRulePropagation(env, canxTq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v2)
		s.waitForVersionSetPropagation(env, tq, v2)
	}
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, newVersioning, v1, "", nil)

	// start workers for v2
	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()
	w2xTq := worker.New(env.SdkClient(), canxTq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2xTq.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2xTq.Start())
	defer w2xTq.Stop()

	if !newVersioning {
		// wait for w1 long polls to all time out
		time.Sleep(longPollTime) //nolint:forbidigo
	}

	// unblock the workflow. it should get kicked off the sticky queue and replay on v1
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))
	// wait for it to start on v1
	env.WaitForChannel(started1)

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(env, run.GetID(), "", v2, newVersioning, v2, "", nil)
	if newVersioning {
		s.validateWorkflowEventsVersionStamps(env, run.GetID(), "", []string{v2}, "")
	}
}

func (s *VersioningIntegSuite) TestDispatchContinueAsNewUpgradeOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchContinueAsNewUpgrade(env, false) })
}

func (s *VersioningIntegSuite) TestDispatchContinueAsNewUpgrade() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchContinueAsNewUpgrade(env, true) })
}

func (s *VersioningIntegSuite) dispatchContinueAsNewUpgrade(
	env *testcore.TestEnv,
	newVersioning bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
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

	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v1)
		s.waitForAssignmentRulePropagation(env, tq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v1)
		s.waitForVersionSetPropagation(env, tq, v1)
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	env.WaitForChannel(started1)

	// now register v11 as newer compatible with v1 AND v2 as a new default
	if newVersioning {
		rule := s.addRedirectRule(env, tq, v1, v11)
		s.waitForRedirectRulePropagation(env, tq, rule)
		rule2 := s.addAssignmentRule(env, tq, v2)
		s.waitForAssignmentRulePropagation(env, tq, rule2)
	} else {
		s.addCompatibleBuildID(env, tq, v11, v1, false)
		s.addNewDefaultBuildID(env, tq, v2)
		s.waitForVersionSetPropagation(env, tq, v2)
	}
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	// start workers for v11 and v2
	w11 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	if !newVersioning {
		// wait for w1 long polls to all time out
		time.Sleep(longPollTime) //nolint:forbidigo
	}

	// unblock the workflow. it should get kicked off the sticky queue and replay on v11
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))
	env.WaitForChannel(started11)

	// then continue-as-new onto v11
	env.WaitForChannel(started11)

	// initial run
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v11, newVersioning, v11, "", []string{v1})

	inheritedBuildId := ""
	if newVersioning {
		inheritedBuildId = v11
	}
	// first CaN
	s.validateWorkflowBuildIds(env, run.GetID(), "", v11, newVersioning, v11, inheritedBuildId, nil)

	// unblock the second run. it should continue on v11 then continue-as-new onto v2, then
	// complete.
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))

	// second CaN
	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(env, run.GetID(), "", v2, newVersioning, v2, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchRetryOld() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchRetryOld(env) })
}

func (s *VersioningIntegSuite) dispatchRetryOld(env *testcore.TestEnv) {
	tq := testcore.RandomizeStr(s.T().Name())
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
			return "", errors.New("try again") //nolint:err113
		case 2:
			return "", errors.New("try again") //nolint:err113
		case 3:
			return "done!", nil
		}
		panic("oops")
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		panic("oops")
	}

	s.addNewDefaultBuildID(env, tq, v1)
	s.waitForVersionSetPropagation(env, tq, v1)

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1000 * time.Millisecond,
		},
	}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	env.WaitForChannel(started1)

	// now register v11 as newer compatible with v1 AND v2 as a new default
	s.addCompatibleBuildID(env, tq, v11, v1, false)
	s.addNewDefaultBuildID(env, tq, v2)
	s.waitForVersionSetPropagation(env, tq, v2)
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	// start workers for v11 and v2
	w11 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime) //nolint:forbidigo

	// unblock the workflow. it should replay on v11 and then retry (on v11).
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))
	env.WaitForChannel(started11) // replay
	env.WaitForChannel(started11) // attempt 2

	// now it's blocked in attempt 2. unblock it.
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))

	// wait for attempt 3. unblock that and it should return.
	env.WaitForChannel(started11) // attempt 3
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v11, false, v11, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchRetry() {
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchRetry(env) })
}

func (s *VersioningIntegSuite) dispatchRetry(env *testcore.TestEnv) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	started1 := make(chan struct{}, 10)
	started2 := make(chan struct{}, 10)

	wf1 := func(ctx workflow.Context) (string, error) {
		started1 <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		if workflow.GetInfo(ctx).Attempt == 1 {
			return "", errors.New("try again") //nolint:err113
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
			return "", errors.New("try again") //nolint:err113
		case 3:
			return "done!", nil
		}
		panic("oops")
	}

	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1000 * time.Millisecond,
		},
	}, "wf")
	s.NoError(err)
	// wait for it to start on v1
	env.WaitForChannel(started1)

	// now register v2 as a new default
	rule = s.addAssignmentRule(env, tq, v2)
	s.waitForAssignmentRulePropagation(env, tq, rule)
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// unblock the workflow on v1
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))

	env.WaitForChannel(started2) // attempt 2
	// now it's blocked in attempt 2. unblock it.
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))

	// wait for attempt 3. unblock that and it should return.
	env.WaitForChannel(started2) // attempt 3
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))

	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v2, true, v2, "", nil)
}

func (s *VersioningIntegSuite) TestDispatchCronOld() {
	s.T().Skip("Skipping test since this tests old versioning behavior and also flakes")
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchCron(env, false) })
}

func (s *VersioningIntegSuite) TestDispatchCron() {
	s.T().Skip("Skipping test since this tests old versioning behavior and also flakes")
	s.runTestWithMatchingBehavior(func(env *testcore.TestEnv, s *VersioningIntegSuite) { s.dispatchCron(env, true) })
}

func (s *VersioningIntegSuite) dispatchCron(
	env *testcore.TestEnv,
	newVersioning bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	v2 := s.prefixed("v2")

	var runIDs1 []string
	var runIDs2 []string

	var runs1 atomic.Int32
	var runs11 atomic.Int32
	var runs2 atomic.Int32

	wf1 := func(ctx workflow.Context) (string, error) {
		runs1.Add(1)
		runIDs1 = append(runIDs1, workflow.GetInfo(ctx).WorkflowExecution.RunID)
		return "ok", nil
	}
	wf11 := func(ctx workflow.Context) (string, error) {
		runs11.Add(1)
		return "ok", nil
	}
	wf2 := func(ctx workflow.Context) (string, error) {
		runs2.Add(1)
		runIDs2 = append(runIDs2, workflow.GetInfo(ctx).WorkflowExecution.RunID)
		return "ok", nil
	}

	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v1)
		s.waitForAssignmentRulePropagation(env, tq, rule)
	} else {
		s.addNewDefaultBuildID(env, tq, v1)
		s.waitForVersionSetPropagation(env, tq, v1)
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v1,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflowWithOptions(wf1, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue:                tq,
		CronSchedule:             "@every 1s",
		WorkflowExecutionTimeout: 7 * time.Second,
	}, "wf")
	s.NoError(err)

	// give it >=3 runs on v1
	s.AwaitTrue(
		func() bool {
			return runs1.Load() >= int32(3)
		},
		6*time.Second,
		100*time.Millisecond,
	)

	if newVersioning {
		rule := s.addAssignmentRule(env, tq, v2)
		s.waitForAssignmentRulePropagation(env, tq, rule)
	} else {
		// now register v11 as newer compatible with v1 AND v2 as a new default.
		// it will run on v2 instead of v11 because cron always starts on default.
		s.addCompatibleBuildID(env, tq, v11, v1, false)
		s.addNewDefaultBuildID(env, tq, v2)
		s.waitForVersionSetPropagation(env, tq, v2)
	}

	// start workers for v11 and v2
	w11 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v11,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w11.RegisterWorkflowWithOptions(wf11, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          v2,
		UseBuildIDForVersioning:          true,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w2.RegisterWorkflowWithOptions(wf2, workflow.RegisterOptions{Name: "wf"})
	s.NoError(w2.Start())

	// give it >=3 runs on v2
	s.AwaitTrue(
		func() bool {
			return runs2.Load() >= int32(3)
		},
		3500*time.Millisecond,
		100*time.Millisecond,
	)
	w2.Stop() // stop w2, because appending to runIDs2 while reading from it to validate causes a race
	s.Zero(runs11.Load())

	for _, runid := range runIDs1 {
		s.validateWorkflowBuildIds(env, run.GetID(), runid, v1, newVersioning, v1, "", nil)
	}
	for _, runid := range runIDs2 {
		s.validateWorkflowBuildIds(env, run.GetID(), runid, v2, newVersioning, v2, "", nil)
	}
}

func (s *VersioningIntegSuite) TestResetWorkflowAssignsToCorrectBuildId() {
	env := s.setupEnv()
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

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

	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	w1 := worker.New(
		env.SdkClient(), tq, worker.Options{
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
		env.SdkClient(), tq, worker.Options{
			BuildID:                          v2,
			UseBuildIDForVersioning:          true,
			MaxConcurrentWorkflowTaskPollers: numPollers,
		},
	)
	w2.RegisterWorkflow(wf)
	w2.RegisterActivity(act1)
	s.NoError(w2.Start())
	defer w2.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	s.validateBuildIDAfterReset(env, run.GetID(), run.GetRunID(), false)
}

func (s *VersioningIntegSuite) TestResetWorkflowAssignsToCorrectBuildId_CaN_Inherit() {
	env := s.setupEnv()
	s.resetWorkflowAssignsToCorrectBuildIDCan(env, true)
}

func (s *VersioningIntegSuite) TestResetWorkflowAssignsToCorrectBuildId_CaN_NoInherit() {
	env := s.setupEnv()
	s.resetWorkflowAssignsToCorrectBuildIDCan(env, false)
}

func (s *VersioningIntegSuite) resetWorkflowAssignsToCorrectBuildIDCan(
	env *testcore.TestEnv,
	inheritBuildID bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	act1 := func() (string, error) {
		return "act1 done!", nil
	}

	wf := func(ctx workflow.Context, attempt int) (string, error) {
		switch attempt {
		case 1:
			intent := temporal.VersioningIntentDefault
			if inheritBuildID {
				intent = temporal.VersioningIntentCompatible
			}
			newCtx := workflow.WithWorkflowVersioningIntent(ctx, intent)
			return "", workflow.NewContinueAsNewError(newCtx, "wf", 2)
		case 2:
			if workflow.GetInfo(ctx).Attempt == 1 {
				// failing first attempt of the CaN so we test inherit behavior across retry attempts
				return "", errors.New("try again") //nolint:err113
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

	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	w1 := worker.New(
		env.SdkClient(), tq, worker.Options{
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
		env.SdkClient(), tq, worker.Options{
			BuildID:                          v2,
			UseBuildIDForVersioning:          true,
			MaxConcurrentWorkflowTaskPollers: numPollers,
		},
	)
	w2.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: "wf"})
	w2.RegisterActivity(act1)
	s.NoError(w2.Start())
	defer w2.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
		TaskQueue: tq,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1000 * time.Millisecond,
		},
	}, wf, 1)
	s.NoError(err)
	s.validateBuildIDAfterReset(env, run.GetID(), "", inheritBuildID)
}

func (s *VersioningIntegSuite) TestResetWorkflowAssignsToCorrectBuildId_ChildWF_Inherit() {
	env := s.setupEnv()
	s.resetWorkflowAssignsToCorrectBuildIDChildWf(env, true)
}

func (s *VersioningIntegSuite) TestResetWorkflowAssignsToCorrectBuildId_ChildWF_NoInherit() {
	env := s.setupEnv()
	s.resetWorkflowAssignsToCorrectBuildIDChildWf(env, false)
}

func (s *VersioningIntegSuite) resetWorkflowAssignsToCorrectBuildIDChildWf(
	env *testcore.TestEnv,
	inheritBuildID bool,
) {
	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	act1 := func() (string, error) {
		return "act1 done!", nil
	}

	var childWfId string
	childStarted := make(chan struct{})

	child := func(ctx workflow.Context) (string, error) {
		if workflow.GetInfo(ctx).Attempt == 1 {
			// failing first attempt of so we test inherit behavior across retry attempts
			return "", errors.New("try again") //nolint:err113
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
		if inheritBuildID {
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

	rule := s.addAssignmentRule(env, tq, v1)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	w1 := worker.New(
		env.SdkClient(), tq, worker.Options{
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
		env.SdkClient(), tq, worker.Options{
			BuildID:                          v2,
			UseBuildIDForVersioning:          true,
			MaxConcurrentWorkflowTaskPollers: numPollers,
		},
	)
	w2.RegisterWorkflow(child)
	w2.RegisterActivity(act1)
	s.NoError(w2.Start())
	defer w2.Stop()

	_, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	env.WaitForChannel(childStarted)
	s.validateBuildIDAfterReset(env, childWfId, "", inheritBuildID)
}

// assumes given run has a single activity task.
func (s *VersioningIntegSuite) validateBuildIDAfterReset(
	env *testcore.TestEnv,
	wfID, runID string, expectedInherit bool,
) {
	v1 := s.prefixed("v1")
	v2 := s.prefixed("v2")

	run := env.SdkClient().GetWorkflow(s.Context(), wfID, runID)

	// let the original run finish
	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("done!", out)

	dw, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), run.GetRunID())
	s.NoError(err)
	tq := dw.GetWorkflowExecutionInfo().GetTaskQueue()
	inheritedBuildId := dw.GetWorkflowExecutionInfo().GetInheritedBuildId()
	s.Equal(expectedInherit, inheritedBuildId != "")
	s.validateWorkflowBuildIds(env, run.GetID(), run.GetRunID(), v1, true, v1, inheritedBuildId, nil)

	// update rules with v2 as the default build
	rule := s.addAssignmentRule(env, tq, v2)
	s.waitForAssignmentRulePropagation(env, tq, rule)

	// now reset the wf to first wf task
	wfr, err := env.SdkClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
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
	run2 := env.SdkClient().GetWorkflow(s.Context(), run.GetID(), wfr.GetRunId())
	s.NoError(run2.Get(s.Context(), &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(env, run2.GetID(), run2.GetRunID(), expectedBuildId, true, expectedBuildId, inheritedBuildId, nil)
	s.validateWorkflowEventsVersionStamps(env, run2.GetID(), run2.GetRunID(), []string{
		expectedBuildId,
		expectedBuildId,
		// expectedBuildId, skipped because it belongs to a sticky queue
	},
		inheritedBuildId)

	// now reset the original wf to second wf task and make sure it remains in v1
	wfr, err = env.SdkClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		},
		WorkflowTaskFinishEventId: 9,
	})
	s.NoError(err)

	run3 := env.SdkClient().GetWorkflow(s.Context(), run.GetID(), wfr.GetRunId())
	s.NoError(run3.Get(s.Context(), &out))
	s.Equal("done!", out)
	s.validateWorkflowBuildIds(env, run3.GetID(), run3.GetRunID(), v1, true, v1, inheritedBuildId, nil)
	s.validateWorkflowEventsVersionStamps(env, run3.GetID(), run3.GetRunID(), []string{v1, v1, v1}, inheritedBuildId)
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueEnhanced_Versioned_ReachabilityCache() {
	env := s.setupEnv()
	tq := testcore.RandomizeStr(s.T().Name())

	// 1. Add assignment rule A and start workflow with build id A
	s.addAssignmentRule(env, tq, "A")
	started := make(chan struct{}, 10)
	wf := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		if workflow.GetInfo(ctx).Attempt == 1 {
			return "", errors.New("try again") //nolint:err113
		}
		panic("oops")
	}
	wId := testcore.RandomizeStr("id")
	w := worker.New(env.SdkClient(), tq, worker.Options{
		UseBuildIDForVersioning: true,
		BuildID:                 "A",
		Identity:                wId,
	})
	w.RegisterWorkflow(wf)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	env.WaitForChannel(started)

	// 2. Wait for visibility to show A as running with BuildId SearchAttribute 'assigned:A'
	s.Await(func(s *VersioningIntegSuite) {
		queryARunning := fmt.Sprintf("TaskQueue = '%s' AND BuildIds IN ('assigned:A') AND ExecutionStatus = \"Running\"", tq)
		resp, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     queryARunning,
		})
		s.NoError(err)
		s.Positive(resp.GetCount())
	}, 5*time.Second, 500*time.Millisecond)

	// 3. Commit a different build id --> A should now only be reachable via visibility query
	s.commitBuildID(env, tq, "B", true, s.getVersioningRules(env, tq).GetConflictToken(), true)

	// 4. Query reachability(A) --> reachable by visibility db, populating reachability open WF cache with A: true
	s.getBuildIDReachability(env, tq, &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{"A"}}, map[string]enumspb.BuildIdTaskReachability{
		"A": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, // reachable by visibility (db)
	})

	// 5. Signal workflow with build id A so that it completes
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))

	// 6. Query reachability(A) --> eventually shows closed_only by visibility db (after TTL passes and A is closed in visibility)
	s.AwaitTrue(func() bool {
		return s.checkBuildIDReachability(env, tq, &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{"A"}}, map[string]enumspb.BuildIdTaskReachability{
			"A": enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, // closed_only by visibility db (after TTL)
		})
	}, 5*time.Second, 50*time.Millisecond)
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueEnhanced_Versioned_BasicReachability() {
	env := s.setupEnv()
	tq := testcore.RandomizeStr(s.T().Name())

	s.getBuildIDReachability(env, tq, nil, map[string]enumspb.BuildIdTaskReachability{
		"": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, // reachable because unversioned is default
	})

	s.addAssignmentRule(env, tq, "A")
	s.getBuildIDReachability(env, tq, &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{"", "A"}}, map[string]enumspb.BuildIdTaskReachability{
		"A": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE,   // reachable by default assignment rule
		"":  enumspb.BUILD_ID_TASK_REACHABILITY_UNREACHABLE, // unreachable because no longer default
	})

	// start workflow and worker with new default assignment rule "A", and wait for it to start
	started := make(chan struct{}, 10)
	wf := func(ctx workflow.Context) (string, error) {
		started <- struct{}{}
		workflow.GetSignalChannel(ctx, "wait").Receive(ctx, nil)
		if workflow.GetInfo(ctx).Attempt == 1 {
			return "", errors.New("try again") //nolint:err113
		}
		panic("oops")
	}
	wId := testcore.RandomizeStr("id")
	w := worker.New(env.SdkClient(), tq, worker.Options{
		UseBuildIDForVersioning: true,
		BuildID:                 "A",
		Identity:                wId,
	})
	w.RegisterWorkflow(wf)
	s.NoError(w.Start())
	defer w.Stop()
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	env.WaitForChannel(started)

	// wait for visibility to show A as running with BuildId SearchAttribute 'assigned:A'
	s.Await(func(s *VersioningIntegSuite) {
		queryARunning := fmt.Sprintf("TaskQueue = '%s' AND BuildIds IN ('assigned:A') AND ExecutionStatus = \"Running\"", tq)
		resp, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     queryARunning,
		})
		s.NoError(err)
		s.Positive(resp.GetCount())
	}, 3*time.Second, 500*time.Millisecond)

	// commit a different build ID --> A should now only be reachable via visibility query, B reachable as default
	s.commitBuildID(env, tq, "B", true, s.getVersioningRules(env, tq).GetConflictToken(), true)
	s.getBuildIDReachability(env, tq, nil, map[string]enumspb.BuildIdTaskReachability{
		"B": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, // reachable by default assignment rule
	})
	s.getBuildIDReachability(env, tq, &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{"A", "B"}}, map[string]enumspb.BuildIdTaskReachability{
		"A": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, // reachable by visibility
		"B": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, // reachable by default assignment rule
	})

	// unblock the workflow on A so that it closes
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))

	// Query reachability(A) --> eventually shows closed_only by visibility db (after TTL passes and A is closed in visibility)
	s.Await(func(s *VersioningIntegSuite) {
		s.True(s.checkBuildIDReachability(env, tq, &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{"A"}}, map[string]enumspb.BuildIdTaskReachability{
			"A": enumspb.BUILD_ID_TASK_REACHABILITY_CLOSED_WORKFLOWS_ONLY, // closed_only by visibility db (after TTL)
			"B": enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE,             // reachable by default assignment rule
		}))
	}, 5*time.Second, 500*time.Millisecond)
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueEnhanced_Unversioned() {
	env := s.setupEnv()
	tq := testcore.RandomizeStr(s.T().Name())
	wf := func(ctx workflow.Context) (string, error) { return "ok", nil }

	workerN := 3
	workerMap := make(map[string]worker.Worker)
	for range workerN {
		wId := testcore.RandomizeStr("id")
		w := worker.New(env.SdkClient(), tq, worker.Options{
			UseBuildIDForVersioning: false,
			Identity:                wId,
		})
		w.RegisterWorkflow(wf)
		s.NoError(w.Start())
		defer w.Stop()
		workerMap[wId] = w
	}

	s.Await(func(s *VersioningIntegSuite) {
		resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:              env.Namespace().String(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			Versions:               nil, // default version, in this case unversioned queue
			TaskQueueTypes:         nil, // both types
			ReportPollers:          true,
			ReportTaskReachability: true,
			ReportStats:            false,
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetVersionsInfo(), 1, "should be 1 because only default/unversioned queue") //nolint:staticcheck
		versionInfo := resp.GetVersionsInfo()[""]
		s.Equal(enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, versionInfo.GetTaskReachability())
		var pollersInfo []*taskqueuepb.PollerInfo
		for _, t := range versionInfo.GetTypesInfo() {
			pollersInfo = append(pollersInfo, t.GetPollers()...)
		}
		foundN := 0
		for wId := range workerMap {
			for _, pi := range pollersInfo {
				//nolint:staticcheck // SA1019: this test covers legacy build-ID versioning.
				s.False(pi.GetWorkerVersionCapabilities().GetUseVersioning())
				if pi.GetIdentity() == wId {
					foundN++
					break
				}
			}
		}

		s.Equal(workerN, foundN)
	}, 3*time.Second, 500*time.Millisecond)
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueEnhanced_ReportFlags() {
	env := s.setupEnv()
	tq := testcore.RandomizeStr(s.T().Name())
	wf := func(ctx workflow.Context) (string, error) { return "ok", nil }

	wId := testcore.RandomizeStr("id")
	w := worker.New(env.SdkClient(), tq, worker.Options{
		UseBuildIDForVersioning: false,
		Identity:                wId,
	})
	w.RegisterWorkflow(wf)
	s.NoError(w.Start())
	defer w.Stop()

	// wait for pollers to show up, verify both ReportPollers and ReportTaskReachability
	s.Await(func(s *VersioningIntegSuite) {
		resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:              env.Namespace().String(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
			Versions:               nil, // default version, in this case unversioned queue
			TaskQueueTypes:         nil, // both types
			ReportPollers:          true,
			ReportTaskReachability: true,
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetVersionsInfo(), 1, "should be 1 because only default/unversioned queue") //nolint:staticcheck
		versionInfo := resp.GetVersionsInfo()[""]
		s.Equal(enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, versionInfo.GetTaskReachability())
		var pollersInfo []*taskqueuepb.PollerInfo
		for _, t := range versionInfo.GetTypesInfo() {
			pollersInfo = append(pollersInfo, t.GetPollers()...)
		}
		foundPoller := false
		for _, pi := range pollersInfo {
			//nolint:staticcheck // SA1019: this test covers legacy build-ID versioning.
			s.False(pi.GetWorkerVersionCapabilities().GetUseVersioning())
			if pi.GetIdentity() == wId {
				foundPoller = true
				break
			}
		}
		s.True(foundPoller)
	}, 3*time.Second, 500*time.Millisecond)

	// ask for reachability only
	resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:               nil, // default version, in this case unversioned queue
		TaskQueueTypes:         nil, // both types
		ReportTaskReachability: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.Len(resp.GetVersionsInfo(), 1, "should be 1 because only default/unversioned queue") //nolint:staticcheck
	versionInfo := resp.GetVersionsInfo()[""]
	s.Equal(enumspb.BUILD_ID_TASK_REACHABILITY_REACHABLE, versionInfo.GetTaskReachability())
	for _, t := range versionInfo.GetTypesInfo() {
		s.Empty(t.GetPollers(), "poller info should not be reported")
	}

	// ask for pollers only
	resp, err = env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
		Namespace:      env.Namespace().String(),
		TaskQueue:      &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:        enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:       nil, // default version, in this case unversioned queue
		TaskQueueTypes: nil, // both types
		ReportPollers:  true,
	})
	s.NoError(err)
	s.NotNil(resp)
	s.Len(resp.GetVersionsInfo(), 1, "should be 1 because only default/unversioned queue") //nolint:staticcheck
	versionInfo = resp.GetVersionsInfo()[""]
	s.Equal(enumspb.BUILD_ID_TASK_REACHABILITY_UNSPECIFIED, versionInfo.GetTaskReachability())
	for _, t := range versionInfo.GetTypesInfo() {
		s.Len(t.GetPollers(), 1, "only one poller info should be reported")
	}
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueEnhanced_TooManyBuildIds() {
	env := s.setupEnv()
	tq := testcore.RandomizeStr(s.T().Name())

	buildIDs := []string{"A", "B", "C", "D"}
	resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:               &taskqueuepb.TaskQueueVersionSelection{BuildIds: buildIDs},
		TaskQueueTypes:         nil, // both types
		ReportPollers:          false,
		ReportTaskReachability: true,
	})
	s.NoError(err)
	s.NotNil(resp)

	buildIDs = []string{"A", "B", "C", "D", "E"}
	resp, err = env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:               &taskqueuepb.TaskQueueVersionSelection{BuildIds: buildIDs},
		TaskQueueTypes:         nil, // both types
		ReportPollers:          false,
		ReportTaskReachability: true,
	})
	s.Error(err)
	s.Nil(resp)
}

func (s *VersioningIntegSuite) TestDescribeTaskQueueLegacy_VersionSets() {
	// force one partition since DescribeTaskQueue only goes to the root
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	)

	tq := testcore.RandomizeStr(s.T().Name())
	v1 := s.prefixed("v1")
	v11 := s.prefixed("v11")
	v2 := s.prefixed("v2")

	wf := func(ctx workflow.Context) (string, error) { return "ok", nil }

	s.addNewDefaultBuildID(env, tq, v1)
	s.addCompatibleBuildID(env, tq, v11, v1, false)
	s.addNewDefaultBuildID(env, tq, v2)
	s.waitForVersionSetPropagation(env, tq, v2)

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                 v1,
		UseBuildIDForVersioning: true,
		Identity:                testcore.RandomizeStr("id"),
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	w11 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                 v11,
		UseBuildIDForVersioning: true,
		Identity:                testcore.RandomizeStr("id"),
	})
	w11.RegisterWorkflow(wf)
	s.NoError(w11.Start())
	defer w11.Stop()

	w2 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                 v2,
		UseBuildIDForVersioning: true,
		Identity:                testcore.RandomizeStr("id"),
	})
	w2.RegisterWorkflow(wf)
	s.NoError(w2.Start())
	defer w2.Stop()

	s.Await(func(s *VersioningIntegSuite) {
		resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace:     env.Namespace().String(),
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
		s.False(havePoller(v1))
		s.True(havePoller(v11))
		s.True(havePoller(v2))
	}, 3*time.Second, 500*time.Millisecond)
}

func (s *VersioningIntegSuite) TestDescribeWorkflowExecution() {
	env := s.setupEnv(
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 4),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 4),
	)

	tq := testcore.RandomizeStr(s.T().Name())
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

	s.addNewDefaultBuildID(env, tq, v1)
	s.waitForVersionSetPropagation(env, tq, v1)

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                 v1,
		UseBuildIDForVersioning: true,
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{TaskQueue: tq}, wf)
	s.NoError(err)
	// wait for it to start on v1
	env.WaitForChannel(started1)

	// describe and check build ID
	s.Await(func(s *VersioningIntegSuite) {
		resp, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), "")
		s.NoError(err)
		//nolint:staticcheck // SA1019: this test covers legacy build-ID versioning.
		s.Equal(v1, resp.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId())
	}, 5*time.Second, 500*time.Millisecond)

	// now register v11 as newer compatible with v1
	s.addCompatibleBuildID(env, tq, v11, v1, false)
	s.waitForVersionSetPropagation(env, tq, v11)
	// add another 100ms to make sure it got to sticky queues also
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	// start worker for v11
	w11 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                 v11,
		UseBuildIDForVersioning: true,
	})
	w11.RegisterWorkflow(wf)
	s.NoError(w11.Start())
	defer w11.Stop()

	// wait for w1 long polls to all time out
	time.Sleep(longPollTime) //nolint:forbidigo

	// unblock the workflow. it should get kicked off the sticky queue and replay on v11
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))
	env.WaitForChannel(started11)

	s.Await(func(s *VersioningIntegSuite) {
		resp, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), run.GetID(), "")
		s.NoError(err)
		//nolint:staticcheck // SA1019: this test covers legacy build-ID versioning.
		s.Equal(v11, resp.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId())
	}, 5*time.Second, 500*time.Millisecond)

	// unblock. it should complete
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), "", "wait", nil))
	var out string
	s.NoError(run.Get(s.Context(), &out))
	s.Equal("ok", out)
}

// Add a per test prefix to avoid hitting the namespace limit of mapped task queue per build ID
func (s *VersioningIntegSuite) prefixed(buildID string) string {
	return fmt.Sprintf("t%x:%s", 0xffff&farm.Hash32([]byte(s.T().Name())), buildID)
}

// listVersioningRules lists rules and checks that the result is successful, returning the response.
func (s *VersioningIntegSuite) getVersioningRules(
	env *testcore.TestEnv,
	tq string) *workflowservice.GetWorkerVersioningRulesResponse {
	res, err := env.FrontendClient().GetWorkerVersioningRules(s.Context(), &workflowservice.GetWorkerVersioningRulesRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(res)
	return res
}

// insertAssignmentRule replaces a new versioning assignment rule to the task queue user data.
// It checks the response and returns the conflict token.
func (s *VersioningIntegSuite) insertAssignmentRule(
	env *testcore.TestEnv,
	tq, newBuildID string,
	idx int32, conflictToken []byte, expectSuccess bool) []byte {
	res, err := env.FrontendClient().UpdateWorkerVersioningRules(s.Context(), &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     env.Namespace().String(),
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertAssignmentRule{
			InsertAssignmentRule: &workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule{
				RuleIndex: idx,
				Rule: &taskqueuepb.BuildIdAssignmentRule{
					TargetBuildId: newBuildID,
				},
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		s.Equal(newBuildID, res.GetAssignmentRules()[idx].GetRule().GetTargetBuildId())
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
	env *testcore.TestEnv,
	tq, newBuildID string,
	idx int32, conflictToken []byte, expectSuccess bool) []byte {
	res, err := env.FrontendClient().UpdateWorkerVersioningRules(s.Context(), &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     env.Namespace().String(),
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceAssignmentRule{
			ReplaceAssignmentRule: &workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule{
				RuleIndex: idx,
				Rule: &taskqueuepb.BuildIdAssignmentRule{
					TargetBuildId: newBuildID,
				},
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		s.Equal(newBuildID, res.GetAssignmentRules()[idx].GetRule().GetTargetBuildId())
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
	env *testcore.TestEnv,
	tq string,
	idx int32, conflictToken []byte, expectSuccess bool) []byte {
	getResp, err := env.FrontendClient().GetWorkerVersioningRules(s.Context(), &workflowservice.GetWorkerVersioningRulesRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
	})
	s.NoError(err)
	s.NotNil(getResp)

	var prevRule *taskqueuepb.BuildIdAssignmentRule
	if expectSuccess {
		prevRule = getResp.GetAssignmentRules()[idx].GetRule()
	}

	res, err := env.FrontendClient().UpdateWorkerVersioningRules(s.Context(), &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     env.Namespace().String(),
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
		s.False(found)
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
	env *testcore.TestEnv,
	tq, sourceBuildID, targetBuildID string,
	conflictToken []byte, expectSuccess bool) []byte {
	res, err := env.FrontendClient().UpdateWorkerVersioningRules(s.Context(), &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     env.Namespace().String(),
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleRedirectRule{
			AddCompatibleRedirectRule: &workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule{
				Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{
					SourceBuildId: sourceBuildID,
					TargetBuildId: targetBuildID,
				},
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		found := false
		for _, r := range res.GetCompatibleRedirectRules() {
			if r.GetRule().GetSourceBuildId() == sourceBuildID && r.GetRule().GetTargetBuildId() == targetBuildID {
				found = true
				break
			}
		}
		s.True(found)
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
	env *testcore.TestEnv,
	tq, sourceBuildID, targetBuildID string,
	conflictToken []byte, expectSuccess bool) []byte {
	res, err := env.FrontendClient().UpdateWorkerVersioningRules(s.Context(), &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     env.Namespace().String(),
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleRedirectRule{
			ReplaceCompatibleRedirectRule: &workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule{
				Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{
					SourceBuildId: sourceBuildID,
					TargetBuildId: targetBuildID,
				},
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		found := false
		for _, r := range res.GetCompatibleRedirectRules() {
			if r.GetRule().GetSourceBuildId() == sourceBuildID && r.GetRule().GetTargetBuildId() == targetBuildID {
				found = true
				break
			}
		}
		s.True(found)
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
	env *testcore.TestEnv,
	tq, sourceBuildID string,
	conflictToken []byte, expectSuccess bool) []byte {
	res, err := env.FrontendClient().UpdateWorkerVersioningRules(s.Context(), &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     env.Namespace().String(),
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleRedirectRule{
			DeleteCompatibleRedirectRule: &workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule{
				SourceBuildId: sourceBuildID,
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		found := false
		for _, r := range res.GetCompatibleRedirectRules() {
			if r.GetRule().GetSourceBuildId() == sourceBuildID {
				found = true
				break
			}
		}
		s.False(found)
		return res.GetConflictToken()
	} else {
		s.Error(err)
		s.Nil(res)
		return nil
	}
}

// commitBuildID sends a CommitBuildId request for the given build ID
// It checks the response and returns the conflict token.
func (s *VersioningIntegSuite) commitBuildID(
	env *testcore.TestEnv,
	tq, targetBuildID string, force bool,
	conflictToken []byte, expectSuccess bool) []byte {
	res, err := env.FrontendClient().UpdateWorkerVersioningRules(s.Context(), &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     env.Namespace().String(),
		TaskQueue:     tq,
		ConflictToken: conflictToken,
		Operation: &workflowservice.UpdateWorkerVersioningRulesRequest_CommitBuildId_{
			CommitBuildId: &workflowservice.UpdateWorkerVersioningRulesRequest_CommitBuildId{
				TargetBuildId: targetBuildID,
				Force:         force,
			},
		},
	})
	if expectSuccess {
		s.NoError(err)
		s.NotNil(res)
		// 1. Adds a fully-ramped assignment rule for the target Build ID at the end of the list.
		endIdx := len(res.GetAssignmentRules()) - 1
		addedRule := res.GetAssignmentRules()[endIdx].GetRule()
		s.Equal(targetBuildID, addedRule.GetTargetBuildId())
		s.InEpsilon(float32(100), addedRule.GetPercentageRamp().GetRampPercentage(), 0.001)

		foundOtherAssignmentRuleForTarget := false
		foundFullyRampedAssignmentRuleForOtherTarget := false
		for i, r := range res.GetAssignmentRules() {
			if r.GetRule().GetTargetBuildId() == targetBuildID && i != endIdx {
				foundOtherAssignmentRuleForTarget = true
			}
			if r.GetRule().GetPercentageRamp().GetRampPercentage() == 100 && r.GetRule().GetTargetBuildId() != targetBuildID {
				foundFullyRampedAssignmentRuleForOtherTarget = true
			}
		}
		// 2. Removes all previously added assignment rules to the given target Build ID (if any).
		s.False(foundOtherAssignmentRuleForTarget)
		// 3. Removes any fully-ramped assignment rule for other Build IDs.
		s.False(foundFullyRampedAssignmentRuleForOtherTarget)
		return res.GetConflictToken()
	} else {
		s.Error(err)
		s.Nil(res)
		return nil
	}
}

func (s *VersioningIntegSuite) registerWorkflowAndPollVersionedTaskQueue(
	env *testcore.TestEnv,
	tq, buildID string, useVersioning bool,
) {
	wf := func(ctx workflow.Context) (string, error) {
		return "done!", nil
	}

	w1 := worker.New(env.SdkClient(), tq, worker.Options{
		BuildID:                          buildID,
		UseBuildIDForVersioning:          useVersioning,
		MaxConcurrentWorkflowTaskPollers: numPollers,
	})
	w1.RegisterWorkflow(wf)
	s.NoError(w1.Start())
	defer w1.Stop()

	// wait for it to start polling
	time.Sleep(200 * time.Millisecond) //nolint:forbidigo
}

func (s *VersioningIntegSuite) getBuildIDReachability(
	env *testcore.TestEnv,
	taskQueue string,
	versions *taskqueuepb.TaskQueueVersionSelection,
	expectedReachability map[string]enumspb.BuildIdTaskReachability) {
	resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ApiMode:                enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED,
		Versions:               versions,
		TaskQueueTypes:         nil, // both types
		ReportPollers:          false,
		ReportTaskReachability: true,
	})
	s.NoError(err)
	s.NotNil(resp)
	for buildID, vi := range resp.GetVersionsInfo() { //nolint:staticcheck
		expected, ok := expectedReachability[buildID]
		s.True(ok, "build id %s was not expected", buildID)
		s.Equal(expected, vi.GetTaskReachability(), "build id %s has unexpected reachability", buildID)
	}
}

func (s *VersioningIntegSuite) checkBuildIDReachability(
	env *testcore.TestEnv,
	taskQueue string,
	versions *taskqueuepb.TaskQueueVersionSelection,
	expectedReachability map[string]enumspb.BuildIdTaskReachability) bool {
	resp, err := env.FrontendClient().DescribeTaskQueue(s.Context(), &workflowservice.DescribeTaskQueueRequest{
		Namespace:              env.Namespace().String(),
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
	for buildID, vi := range resp.GetVersionsInfo() { //nolint:staticcheck
		expected, ok := expectedReachability[buildID]
		if !ok {
			return false // build id was not expected
		}
		if expected != vi.GetTaskReachability() {
			return false // build id has unexpected reachability
		}
	}
	return true
}

// addNewDefaultBuildID updates build ID info on a task queue with a new build ID in a new default set.
func (s *VersioningIntegSuite) addNewDefaultBuildID(
	env *testcore.TestEnv,
	tq, newBuildID string,
) {
	res, err := env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
			AddNewBuildIdInNewDefaultSet: newBuildID,
		},
	})
	s.NoError(err)
	s.NotNil(res)
}

func (s *VersioningIntegSuite) addAssignmentRule(
	env *testcore.TestEnv,
	tq, buildID string,
) *taskqueuepb.BuildIdAssignmentRule {
	return s.addAssignmentRuleWithRamp(env, tq, buildID, 100)
}

func (s *VersioningIntegSuite) addAssignmentRuleWithRamp(
	env *testcore.TestEnv,
	tq, buildID string, ramp float32,
) *taskqueuepb.BuildIdAssignmentRule {
	rule := &taskqueuepb.BuildIdAssignmentRule{
		TargetBuildId: buildID,
		Ramp: &taskqueuepb.BuildIdAssignmentRule_PercentageRamp{
			PercentageRamp: &taskqueuepb.RampByPercentage{
				RampPercentage: ramp,
			},
		},
	}
	return s.doAddAssignmentRule(env, tq, rule)
}

func (s *VersioningIntegSuite) doAddAssignmentRule(
	env *testcore.TestEnv,
	tq string, rule *taskqueuepb.BuildIdAssignmentRule,
) *taskqueuepb.BuildIdAssignmentRule {
	cT := s.getVersioningRules(env, tq).GetConflictToken()
	res, err := env.FrontendClient().UpdateWorkerVersioningRules(s.Context(), &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     env.Namespace().String(),
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

func (s *VersioningIntegSuite) addRedirectRule(
	env *testcore.TestEnv,
	tq, source string, target string,
) *taskqueuepb.CompatibleBuildIdRedirectRule {
	cT := s.getVersioningRules(env, tq).GetConflictToken()
	rule := &taskqueuepb.CompatibleBuildIdRedirectRule{
		SourceBuildId: source,
		TargetBuildId: target,
	}
	res, err := env.FrontendClient().UpdateWorkerVersioningRules(s.Context(), &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     env.Namespace().String(),
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

func (s *VersioningIntegSuite) removeRedirectRule(
	env *testcore.TestEnv,
	tq, source string,
) {
	cT := s.getVersioningRules(env, tq).GetConflictToken()
	res, err := env.FrontendClient().UpdateWorkerVersioningRules(s.Context(), &workflowservice.UpdateWorkerVersioningRulesRequest{
		Namespace:     env.Namespace().String(),
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

// addCompatibleBuildID updates build ID info on a task queue with a new compatible build ID.
func (s *VersioningIntegSuite) addCompatibleBuildID(
	env *testcore.TestEnv,
	tq, newBuildID, existing string, makeSetDefault bool,
) {
	res, err := env.FrontendClient().UpdateWorkerBuildIdCompatibility(s.Context(), &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tq,
		Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
			AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
				NewBuildId:                newBuildID,
				ExistingCompatibleBuildId: existing,
				MakeSetDefault:            makeSetDefault,
			},
		},
	})
	s.NoError(err)
	s.NotNil(res)
}

// waitForVersionSetPropagation waits for all partitions of tq to mention newBuildID in their versioning data (in any position).
func (s *VersioningIntegSuite) waitForVersionSetPropagation(
	env *testcore.TestEnv,
	taskQueue, newBuildID string,
) {
	s.waitForPropagation(env, taskQueue, 0, func(vd *persistencespb.VersioningData) bool {
		for _, set := range vd.GetVersionSets() {
			for _, id := range set.BuildIds {
				if id.Id == newBuildID {
					return true
				}
			}
		}
		return false
	})
}

// waitForAssignmentRulePropagation waits for all partitions of tq to have the given assignment rule in their versioning data
func (s *VersioningIntegSuite) waitForAssignmentRulePropagation(
	env *testcore.TestEnv,
	taskQueue string, rule *taskqueuepb.BuildIdAssignmentRule,
) {
	s.waitForPropagation(env, taskQueue, 0, func(vd *persistencespb.VersioningData) bool {
		for _, r := range vd.GetAssignmentRules() {
			if r.GetRule().Equal(rule) {
				return true
			}
		}
		return false
	})
}

// waitForRedirectRulePropagation waits for all partitions of tq to have the given redirect rule in their versioning data
func (s *VersioningIntegSuite) waitForRedirectRulePropagation(
	env *testcore.TestEnv,
	taskQueue string, rule *taskqueuepb.CompatibleBuildIdRedirectRule,
) {
	s.waitForPropagation(env, taskQueue, 0, func(vd *persistencespb.VersioningData) bool {
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
	env *testcore.TestEnv,
	taskQueue string,
	rule *taskqueuepb.CompatibleBuildIdRedirectRule,
	upToPartition int,
) {
	s.waitForPropagation(env, taskQueue, upToPartition+1, func(vd *persistencespb.VersioningData) bool {
		for _, r := range vd.GetRedirectRules() {
			if r.GetRule().Equal(rule) {
				return true
			}
		}
		return false
	})
}

// waitForPropagation waits for all partitions of tq to mention newBuildID in their versioning data (in any position).
// Pass 0 for partitionCount to make it load from MatchingNumTaskqueueReadPartitions config.
func (s *VersioningIntegSuite) waitForPropagation(
	env *testcore.TestEnv,
	taskQueue string,
	partitionCount int,
	condition func(data *persistencespb.VersioningData) bool,
) {
	if partitionCount <= 0 {
		v := env.GetTestCluster().Host().DcClient().GetValue(dynamicconfig.MatchingNumTaskqueueReadPartitions.Key())
		s.NotEmpty(v, "versioning tests require setting explicit number of partitions")
		count, ok := v[0].Value.(int)
		s.True(ok, "partition count is not an int")
		partitionCount = count
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
	s.Await(func(s *VersioningIntegSuite) {
		for pt := range remaining {
			f, err := tqid.NewTaskQueueFamily(env.NamespaceID().String(), taskQueue)
			s.NoError(err)
			partition := f.TaskQueue(pt.tp).NormalPartition(pt.part)
			// Use lower-level GetTaskQueueUserData instead of GetWorkerBuildIdCompatibility
			// here so that we can target activity queues.
			res, err := env.GetTestCluster().Host().MatchingClient().GetTaskQueueUserData(
				s.Context(),
				&matchingservice.GetTaskQueueUserDataRequest{
					NamespaceId:   env.NamespaceID().String(),
					TaskQueue:     partition.RpcName(),
					TaskQueueType: partition.TaskType(),
				})
			s.NoError(err)
			if condition(res.GetUserData().GetData().GetVersioningData()) {
				delete(remaining, pt)
			}
		}
		s.Empty(remaining)
	}, 10*time.Second, 500*time.Millisecond)
}

func (s *VersioningIntegSuite) unloadTaskQueue(
	env *testcore.TestEnv,
	tq string,
) {
	_, err := env.GetTestCluster().MatchingClient().ForceUnloadTaskQueuePartition(s.Context(), &matchingservice.ForceUnloadTaskQueuePartitionRequest{
		NamespaceId: env.NamespaceID().String(),
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     tq,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		},
	})
	s.NoError(err)
}

func (s *VersioningIntegSuite) getStickyQueueName(
	env *testcore.TestEnv,
	id string,
) string {
	ms, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: id},
		Archetype: chasm.WorkflowArchetype,
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
func (s *VersioningIntegSuite) waitForWorkflowBuildID(
	env *testcore.TestEnv,
	wfID string,
	runID string,
	buildID string,
) {
	s.AwaitTrue(
		func() bool {
			dw, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), wfID, runID)
			if err != nil {
				return false
			}
			return dw.GetWorkflowExecutionInfo().GetAssignedBuildId() == buildID //nolint:staticcheck
		},
		10*time.Second,
		100*time.Millisecond,
	)
}

func (s *VersioningIntegSuite) validateWorkflowBuildIds(
	env *testcore.TestEnv,
	wfID string,
	runID string,
	expectedBuildId string,
	newVersioning bool,
	expectedStampBuildId string,
	expectedInheritedBuildId string,
	extraSearchAttrBuildIds []string,
) {
	dw, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), wfID, runID)
	s.NoError(err)
	saPayload := dw.GetWorkflowExecutionInfo().GetSearchAttributes().GetIndexedFields()["BuildIds"]
	searchAttrAny, err := sadefs.DecodeValue(saPayload, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
	var searchAttr []string
	if searchAttrAny != nil {
		searchAttr = searchAttrAny.([]string)
	}
	s.NoError(err)
	if expectedBuildId == "" {
		if expectedStampBuildId != "" {
			s.NotNil(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetBuildId())
			s.False(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp().GetUseVersioning())
			s.Len(searchAttr, 2+len(extraSearchAttrBuildIds))
			s.Equal(worker_versioning.UnversionedSearchAttribute, searchAttr[0])
			s.True(strings.HasPrefix(searchAttr[1], worker_versioning.UnversionedSearchAttribute))
		} else {
			s.Nil(dw.GetWorkflowExecutionInfo().GetMostRecentWorkerVersionStamp())
			s.Empty(searchAttr)
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
			s.Len(searchAttr, 2+len(extraSearchAttrBuildIds))
			s.Equal(worker_versioning.AssignedBuildIdSearchAttribute(expectedBuildId), searchAttr[0])
			s.Contains(searchAttr, worker_versioning.VersionedBuildIdSearchAttribute(expectedBuildId))
		} else {
			s.Empty(dw.GetWorkflowExecutionInfo().GetAssignedBuildId()) //nolint:staticcheck
			if expectedStampBuildId != "" {
				s.Len(searchAttr, 1+len(extraSearchAttrBuildIds))
				s.Contains(searchAttr, worker_versioning.VersionedBuildIdSearchAttribute(expectedBuildId))
			} else {
				s.Empty(searchAttr)
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
	env *testcore.TestEnv,
	wfID, runID string,
	expectedBuildIds []string,
	expectedInheritedBuildId string,
) {
	wh := env.SdkClient().GetWorkflowHistory(s.Context(), wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	counter := 0
	checkedInheritedBuildId := false
	for wh.HasNext() {
		he, err := wh.Next()
		s.NoError(err)
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
