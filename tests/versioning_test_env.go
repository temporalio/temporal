package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/tests/testcore"
)

type VersioningTestEnv struct {
	*testcore.TestEnv
}

func newVersioningTestEnv(t *testing.T, opts ...testcore.TestOption) *VersioningTestEnv {
	return &VersioningTestEnv{
		TestEnv: newWorkerDeploymentCleanupEnv(t, opts...),
	}
}

func (env *VersioningTestEnv) waitForTaskQueueVersioningInfo(
	ctx context.Context,
	tb testing.TB,
	tq *taskqueuepb.TaskQueue,
	expectedCurrentVersion string,
	expectedRampingVersion string,
	rampingPercentage float32,
) {
	await.Require(ctx, tb, func(t *await.T) {
		resp, err := env.FrontendClient().DescribeTaskQueue(t.Context(), &workflowservice.DescribeTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: tq,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		protorequire.ProtoEqual(t, worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expectedCurrentVersion), resp.GetVersioningInfo().GetCurrentDeploymentVersion())
		protorequire.ProtoEqual(t, worker_versioning.ExternalWorkerDeploymentVersionFromStringV31(expectedRampingVersion), resp.GetVersioningInfo().GetRampingDeploymentVersion())
		require.Equal(t, expectedCurrentVersion, resp.GetVersioningInfo().GetCurrentVersion()) //nolint:staticcheck // SA1019: old worker versioning
		require.Equal(t, expectedRampingVersion, resp.GetVersioningInfo().GetRampingVersion()) //nolint:staticcheck // SA1019: old worker versioning
		require.InDelta(t, rampingPercentage, resp.GetVersioningInfo().GetRampingVersionPercentage(), 0.001)
	}, 10*time.Second, 200*time.Millisecond)
}

func (env *VersioningTestEnv) findVersionTaskQueue(
	taskQueues []*workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue,
	tqName string,
	tqType enumspb.TaskQueueType,
) *workflowservice.DescribeWorkerDeploymentVersionResponse_VersionTaskQueue {
	for _, tq := range taskQueues {
		if tq.GetName() == tqName && tq.GetType() == tqType {
			return tq
		}
	}
	return nil
}
