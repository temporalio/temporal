package tests

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

const (
	maxConcurrentBatchOperations                 = 3
	testVersionDrainageRefreshInterval           = 3 * time.Second
	testVersionDrainageVisibilityGracePeriod     = 3 * time.Second
	testLongVersionDrainageRefreshInterval       = 10 * time.Second
	testLongVersionDrainageVisibilityGracePeriod = 10 * time.Second
	testMaxVersionsInDeployment                  = 4
)

var (
	testRandomMetadataValue = []byte("random metadata value")
)

func pollActivityFromDeployment(ctx context.Context, env *testcore.TestEnv, tv *testvars.TestVars) {
	_, _ = env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace:         env.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          uuid.NewString(),
		DeploymentOptions: tv.WorkerDeploymentOptions(true),
	})
}

func requireWorkerDeploymentMetricTags(
	s parallelsuite.Scope,
	capture *testcore.NamespaceMetricCapture,
	tv *testvars.TestVars,
	metricNames ...string,
) {
	for _, metricName := range metricNames {
		await.Require(s.Context(), s.TB(), func(t *await.T) {
			r := t.Require()
			recordings := capture.CollectMetric(metricName, func(recording *metricstest.CapturedRecording) bool {
				taskQueue, hasTaskQueue := recording.Tags["taskqueue"]
				return !hasTaskQueue || taskQueue == tv.TaskQueue().GetName()
			})
			r.NotEmpty(recordings, "expected %s in namespace %s", metricName, tv.NamespaceName())
			hasExpectedTags := false
			for _, recording := range recordings {
				if recording.Tags["worker_deployment_name"] == tv.DeploymentSeries() &&
					recording.Tags["worker_build_id"] == tv.BuildID() {
					hasExpectedTags = true
					break
				}
			}
			r.True(hasExpectedTags, "expected %s with deployment %q and build ID %q", metricName, tv.DeploymentSeries(), tv.BuildID())
		}, 5*time.Second, 50*time.Millisecond)
	}
}
