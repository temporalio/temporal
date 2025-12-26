package deployment

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.uber.org/mock/gomock"
)

const (
	testReachabilityCacheOpenWFsTTL   = time.Millisecond
	testReachabilityCacheClosedWFsTTL = 3 * time.Millisecond
)

func TestMakeDeploymentQuery(t *testing.T) {
	t.Parallel()
	seriesName := "test-deployment"
	buildId := "A"

	query := makeDeploymentQuery(seriesName, buildId, true)
	expectedQuery := "BuildIds = 'pinned:test-deployment:A' AND ExecutionStatus = 'Running'"
	assert.Equal(t, expectedQuery, query)

	query = makeDeploymentQuery(seriesName, buildId, false)
	expectedQuery = "BuildIds = 'pinned:test-deployment:A' AND ExecutionStatus != 'Running'"
	assert.Equal(t, expectedQuery, query)
}

func TestReachable_CurrentDeployment(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	seriesName := "test-deployment"
	buildId := "A"
	vm := manager.NewMockVisibilityManager(gomock.NewController(t)) // won't receive any calls
	testCache := newReachabilityCache(metrics.NoopMetricsHandler, vm, testReachabilityCacheOpenWFsTTL, testReachabilityCacheClosedWFsTTL)

	reach, _, err := getDeploymentReachability(ctx, "", "", seriesName, buildId, true, testCache)
	assert.NoError(t, err)
	assert.Equal(t, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE, reach)
}

func TestReachable_OpenWorkflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	nsId := "pretend-this-is-a-uuid"
	nsName := "test-namespace"
	seriesName := "test-deployment"
	buildId := "A"
	vm := manager.NewMockVisibilityManager(gomock.NewController(t))
	openCountRequest := makeCountRequest(nsId, nsName, seriesName, buildId, true)
	closedCountRequest := makeCountRequest(nsId, nsName, seriesName, buildId, false)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), &openCountRequest).MaxTimes(2).Return(mkCountResponse(1))
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), &closedCountRequest).MaxTimes(2).Return(mkCountResponse(0))
	testCache := newReachabilityCache(metrics.NoopMetricsHandler, vm, testReachabilityCacheOpenWFsTTL, testReachabilityCacheClosedWFsTTL)

	// put a value in cold cache
	reach, reachValidTime, err := getDeploymentReachability(ctx, nsId, nsName, seriesName, buildId, false, testCache)
	assert.NoError(t, err)
	assert.Greater(t, time.Now(), reachValidTime)
	assert.Equal(t, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE, reach)

	// get the cached value and time
	reach, reachValidTimeCached, err := getDeploymentReachability(ctx, nsId, nsName, seriesName, buildId, false, testCache)
	assert.NoError(t, err)
	assert.Equal(t, reachValidTime, reachValidTimeCached)
	assert.Equal(t, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE, reach)

	// check that the cache is cold again after TTL (as shown by newer valid time)
	time.Sleep(testReachabilityCacheOpenWFsTTL) //nolint:forbidigo
	reach, reachValidTimeCacheCold, err := getDeploymentReachability(ctx, nsId, nsName, seriesName, buildId, false, testCache)
	assert.NoError(t, err)
	assert.Greater(t, reachValidTimeCacheCold, reachValidTime)
	assert.Equal(t, enumspb.DEPLOYMENT_REACHABILITY_REACHABLE, reach)
}

func TestReachable_ClosedWorkflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	nsId := "pretend-this-is-a-uuid"
	nsName := "test-namespace"
	seriesName := "test-deployment"
	buildId := "A"
	vm := manager.NewMockVisibilityManager(gomock.NewController(t))
	openCountRequest := makeCountRequest(nsId, nsName, seriesName, buildId, true)
	closedCountRequest := makeCountRequest(nsId, nsName, seriesName, buildId, false)
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), &openCountRequest).MaxTimes(2).Return(mkCountResponse(0))
	vm.EXPECT().CountWorkflowExecutions(gomock.Any(), &closedCountRequest).MaxTimes(2).Return(mkCountResponse(1))
	testCache := newReachabilityCache(metrics.NoopMetricsHandler, vm, testReachabilityCacheOpenWFsTTL, testReachabilityCacheClosedWFsTTL)

	// put a value in cold cache
	reach, reachValidTime, err := getDeploymentReachability(ctx, nsId, nsName, seriesName, buildId, false, testCache)
	assert.NoError(t, err)
	assert.Greater(t, time.Now(), reachValidTime)
	assert.Equal(t, enumspb.DEPLOYMENT_REACHABILITY_CLOSED_WORKFLOWS_ONLY, reach)

	// get the cached value and time
	reach, reachValidTimeCacheHot, err := getDeploymentReachability(ctx, nsId, nsName, seriesName, buildId, false, testCache)
	assert.NoError(t, err)
	assert.Equal(t, reachValidTime, reachValidTimeCacheHot)
	assert.Equal(t, enumspb.DEPLOYMENT_REACHABILITY_CLOSED_WORKFLOWS_ONLY, reach)

	// check that the cache is cold again after TTL (as shown by newer valid time)
	time.Sleep(testReachabilityCacheClosedWFsTTL) //nolint:forbidigo
	reach, reachValidTimeCacheCold, err := getDeploymentReachability(ctx, nsId, nsName, seriesName, buildId, false, testCache)
	assert.NoError(t, err)
	assert.Greater(t, reachValidTimeCacheCold, reachValidTime)
	assert.Equal(t, enumspb.DEPLOYMENT_REACHABILITY_CLOSED_WORKFLOWS_ONLY, reach)
}

func mkCountResponse(count int64) (*manager.CountWorkflowExecutionsResponse, error) {
	return &manager.CountWorkflowExecutionsResponse{
		Count:  count,
		Groups: nil,
	}, nil
}
