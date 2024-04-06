package matching

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/temporalio/sqlparser"
	"go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/worker_versioning"
)

/*
Redirect Rules:
10
^
|
1 <------ 2
^
|
5 <------ 3 <------ 4
*/
func TestGetUpstreamBuildIds_NoCycle(t *testing.T) {
	t.Parallel()
	redirectRules := []*taskqueuepb.TimestampedCompatibleBuildIdRedirectRule{
		{Rule: mkRedirectRule("1", "10")},
		{Rule: mkRedirectRule("2", "1")},
		{Rule: mkRedirectRule("3", "5")},
		{Rule: mkRedirectRule("4", "3")},
		{Rule: mkRedirectRule("5", "1")},
	}

	expectedUpstreamBuildIds := []string{"2", "5", "3", "4"}
	upstreamBuildIds := getUpstreamBuildIds("1", redirectRules)
	fmt.Printf("%+v\n", upstreamBuildIds)

	for _, bid := range expectedUpstreamBuildIds {
		assert.Contains(t, upstreamBuildIds, bid)
	}
	assert.Equal(t, len(expectedUpstreamBuildIds), len(upstreamBuildIds))
}

func TestGetUpstreamBuildIds_WithCycle(t *testing.T) {
	t.Parallel()
	// todo
}

func TestExistsBackloggedActivityOrWFAssignedTo(t *testing.T) {
	t.Parallel()
	// todo after we have backlog info
}

/*
Assignment Rules:
[ (3, 50%), (2, nil) (1, nil) ]

Expect 3 and 2 are reachable, but not 1 since it is behind an unconditional rule.
*/
func TestIsReachableAssignmentRuleTarget(t *testing.T) {
	t.Parallel()
	assignmentRules := []*taskqueuepb.TimestampedBuildIdAssignmentRule{
		{Rule: mkAssignmentRule("3", mkNewAssignmentPercentageRamp(50))},
		{Rule: mkAssignmentRule("2", nil)},
		{Rule: mkAssignmentRule("1", nil)},
	}

	assert.True(t, isReachableAssignmentRuleTarget("3", assignmentRules))
	assert.True(t, isReachableAssignmentRuleTarget("2", assignmentRules))
	assert.False(t, isReachableAssignmentRuleTarget("1", assignmentRules))
	assert.False(t, isReachableAssignmentRuleTarget("0", assignmentRules))
}

// mock visibility
func TestGetReachability_NoRules(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tq := "test-exists-tq"
	nsID := "test-namespace-id"
	nsName := "test-namespace"

	ctrl := gomock.NewController(t)
	vm := manager.NewMockVisibilityManager(ctrl)
	mockStore := mockVisibilityStore(make(map[mockRunId]mockVisibilityEntry))

	// 1. imagine there is 1 only closed workflows in visibility, with BuildIds == NULL
	// getReachability("") --> CLOSED_ONLY
	queryOpen := makeBuildIdQuery([]string{""}, tq, true)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID(nsID),
		Namespace:   namespace.Name(nsName),
		Query:       queryOpen,
	}).Return(0)
	queryClosed := makeBuildIdQuery([]string{""}, tq, false)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), &manager.CountWorkflowExecutionsRequest{
		NamespaceID: namespace.ID(nsID),
		Namespace:   namespace.Name(nsName),
		Query:       queryClosed,
	}).Return(1)
	existsWFAssignedToAny(ctx, vm, nsID, nsName, makeBuildIdQuery([]string{""}, tq, true))
	exists, _ := existsWFAssignedToAny(ctx, vm, nsID, nsName, tq, []string{""}, true)
	assert.False(t, exists)
	exists, _ = existsWFAssignedToAny(ctx, vm, nsID, nsName, tq, []string{""}, false)
	assert.True(t, exists)

	// 2. getReachability(1) --> UNREACHABLE

	// 3. populate mockStore with a versioned workflow assigned to build id 1
	// now you start a versioned workflow assigned to build id 1, and it is running
	// getReachability(1) --> REACHABLE
	mockStore["run1"] = mockVisibilityEntry{
		taskQueue:              tq,
		buildIdsList:           []string{"'assigned:1'", "'versioned:1'"},
		executionStatusRunning: true,
	}
}

// nothing in visibility for this test
func TestGetReachability_WithActiveRules(t *testing.T) {
	// Assignment: [ A, D ]. Redirect: (A->B), (B->C)

	// getReachability(A) --> unreachable (redirect rule source)
	// getReachability(C) --> reachable (redirect rule target of reachable source)
	// getReachability(D) --> reachable (assignment rule target)
}

// nothing in visibility for this test
func TestGetReachability_WithDeletedRules(t *testing.T) {
	// recently-deleted redirect rule source --> reachable
	// recently-deleted assignment rule target --> reachable
	// deleted assignment rule target --> not reachable
	// recently-deleted redirect rule target of reachable source --> reachable
	// deleted redirect rule target of reachable source --> unreachable

	// Assignment: [ (C, active), (B, recently deleted), (A, deleted) ]. Redirect: (A->B, active), (C->B, recently deleted)

	// todo
	// getReachability(A) --> unreachable (active redirect rule source)
	// getReachability(A) --> unreachable (active redirect rule source)
	// getReachability(C) --> reachable (redirect rule target of reachable source)
	// getReachability(D) --> reachable (assignment rule target)
}

func TestMakeBuildIdQuery(t *testing.T) {
	t.Parallel()
	tq := "test-query-tq"

	buildIdsOfInterest := []string{"0", "1", "2", ""}
	query := makeBuildIdQuery(buildIdsOfInterest, tq, true)
	expectedQuery := "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('assigned:0','assigned:1','assigned:2','unversioned')) AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQuery, query)

	query = makeBuildIdQuery(buildIdsOfInterest, tq, false)
	expectedQuery = "TaskQueue = 'test-query-tq' AND (BuildIds IS NULL OR BuildIds IN ('versioned:0','versioned:1','versioned:2','unversioned')) AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQuery, query)

	buildIdsOfInterest = []string{"0", "1", "2"}
	query = makeBuildIdQuery(buildIdsOfInterest, tq, true)
	expectedQuery = "TaskQueue = 'test-query-tq' AND BuildIds IN ('assigned:0','assigned:1','assigned:2') AND ExecutionStatus = \"Running\""
	assert.Equal(t, expectedQuery, query)

	query = makeBuildIdQuery(buildIdsOfInterest, tq, false)
	expectedQuery = "TaskQueue = 'test-query-tq' AND BuildIds IN ('versioned:0','versioned:1','versioned:2') AND ExecutionStatus != \"Running\""
	assert.Equal(t, expectedQuery, query)
}

// Helpers for mock Visibility Manager
type mockRunId string

type mockVisibilityStore map[mockRunId]mockVisibilityEntry

type mockVisibilityEntry struct {
	taskQueue              string
	buildIdsList           []string
	executionStatusRunning bool
}

func (s mockVisibilityStore) RecordWorkflowExecutionStarted(runId mockRunId, taskQueue string, versionCapabilities *common.WorkerVersionCapabilities) {
	record := mockVisibilityEntry{
		taskQueue:              taskQueue,
		buildIdsList:           nil,
		executionStatusRunning: true,
	}
	if versionCapabilities != nil {
		if versionCapabilities.UseVersioning {
			record.buildIdsList = []string{sqlparser.String(sqlparser.NewStrVal([]byte(worker_versioning.AssignedBuildIdSearchAttribute(versionCapabilities.BuildId))))}
		} else if versionCapabilities.BuildId != "" {
			record.buildIdsList = []string{sqlparser.String(sqlparser.NewStrVal([]byte(worker_versioning.UnversionedBuildIdSearchAttribute(versionCapabilities.BuildId))))}
		}
	}
	s[runId] = record
}

func (s mockVisibilityStore) UpsertWorkflowExecution(runId mockRunId, versionCapabilities *common.WorkerVersionCapabilities) {
}

func (s mockVisibilityStore) RecordWorkflowExecutionClosed(runId mockRunId, versionCapabilities *common.WorkerVersionCapabilities) {
}

func (s mockVisibilityStore) DeleteWorkflowExecution(runId mockRunId, versionCapabilities *common.WorkerVersionCapabilities) {
}

func newMockVisibilityForReachability(t *testing.T) (*manager.MockVisibilityManager, mockVisibilityStore) {
	ctrl := gomock.NewController(t)
	vm := manager.NewMockVisibilityManager(ctrl)
	mockStore := mockVisibilityStore(make(map[mockRunId]mockVisibilityEntry))
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).DoAndReturn(func(
		ctx context.Context,
		request *manager.CountWorkflowExecutionsRequest,
	) (*manager.CountWorkflowExecutionsResponse, error) {
		mockEntry, checkBuildIdNull := parseReachabilityQuery(request.Query)
		count := 0
		for _, e := range mockStore {
			if e.taskQueue == mockEntry.taskQueue {
				if e.executionStatusRunning != mockEntry.executionStatusRunning {
					continue
				}
				if e.buildIdsList == nil {
					if checkBuildIdNull {
						count++
						continue
					}
					continue
				}
				for _, bidSA := range mockEntry.buildIdsList {
					if slices.Contains(e.buildIdsList, bidSA) {
						count++
						break
					}
				}
			}
		}
		return &manager.CountWorkflowExecutionsResponse{Count: int64(count)}, nil
	}).AnyTimes()

	return vm, mockStore
}

func parseReachabilityQuery(query string) (mockVisibilityEntry, bool) {
	ret := mockVisibilityEntry{}
	checkBuildIdNull := false
	for _, c := range strings.Split(query, " AND ") {
		if strings.Contains(c, "TaskQueue") {
			ret.taskQueue = strings.ReplaceAll(strings.Split(c, " = ")[1], "'", "")
		} else if strings.Contains(c, "ExecutionStatus") {
			ret.executionStatusRunning = strings.Contains(c, "ExecutionStatus = \"Running\"")
		} else {
			checkBuildIdNull = strings.Contains(c, "BuildIds IS NULL")
			splits := strings.Split(c, "BuildIds")
			buildIdsIdx := 1
			if len(splits) == 3 {
				buildIdsIdx = 2
			}
			trimFront := strings.ReplaceAll(splits[buildIdsIdx], " IN (", "")
			trimBack := strings.ReplaceAll(trimFront, ")", "")
			ret.buildIdsList = strings.Split(trimBack, ",")
		}
	}
	return ret, checkBuildIdNull
}

func TestParseBuildIdQuery(t *testing.T) {
	t.Parallel()
	tq := "test-parse-tq"

	buildIdsOfInterest := []string{"0", "1", "2", ""}
	mockEntry, checkNull := parseReachabilityQuery(makeBuildIdQuery(buildIdsOfInterest, tq, true))
	expectedMockEntry := mockVisibilityEntry{
		taskQueue:              tq,
		buildIdsList:           []string{"'assigned:0'", "'assigned:1'", "'assigned:2'", "unversioned"},
		executionStatusRunning: true,
	}
	assert.Equal(t, expectedMockEntry, mockEntry)
	assert.True(t, checkNull)

	mockEntry, checkNull = parseReachabilityQuery(makeBuildIdQuery(buildIdsOfInterest, tq, false))
	expectedMockEntry = mockVisibilityEntry{
		taskQueue:              tq,
		buildIdsList:           []string{"'versioned:0'", "'versioned:1'", "'versioned:2'", "unversioned"},
		executionStatusRunning: false,
	}
	assert.Equal(t, expectedMockEntry, mockEntry)
	assert.True(t, checkNull)

	buildIdsOfInterest = []string{"0", "1", "2"}
	mockEntry, checkNull = parseReachabilityQuery(makeBuildIdQuery(buildIdsOfInterest, tq, true))
	expectedMockEntry = mockVisibilityEntry{
		taskQueue:              tq,
		buildIdsList:           []string{"'assigned:0'", "'assigned:1'", "'assigned:2'"},
		executionStatusRunning: true,
	}
	assert.Equal(t, expectedMockEntry, mockEntry)
	assert.False(t, checkNull)

	mockEntry, checkNull = parseReachabilityQuery(makeBuildIdQuery(buildIdsOfInterest, tq, false))
	expectedMockEntry = mockVisibilityEntry{
		taskQueue:              tq,
		buildIdsList:           []string{"'versioned:0'", "'versioned:1'", "'versioned:2'"},
		executionStatusRunning: false,
	}
	assert.Equal(t, expectedMockEntry, mockEntry)
	assert.False(t, checkNull)
}
