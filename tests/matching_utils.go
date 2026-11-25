package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

// runWithMatchingBehaviors runs a test with all combinations of matching behaviors.
func runWithMatchingBehaviors[S interface {
	T() *testing.T
	Run(string, func(S)) bool
}](
	s S,
	baseOpts []testcore.TestOption,
	subtest func(s S, env *testcore.TestEnv, behavior testcore.MatchingBehavior),
) {
	for _, behavior := range testcore.AllMatchingBehaviors() {
		s.Run(behavior.Name(), func(s S) {
			opts := append([]testcore.TestOption{}, baseOpts...)
			opts = append(opts, behavior.Options()...)

			env := testcore.NewEnv(s.T(), opts...)
			behavior.InjectHooks(env)

			subtest(s, env, behavior)
		})
	}
}

// syncDeploymentVersionToTaskQueues sends a SyncDeploymentUserData request to the matching service
// to register a deployment version as current for the specified task queue types, then waits for
// the data to propagate to all partitions using CheckTaskQueueUserDataPropagation.
func syncDeploymentVersionToTaskQueues(
	t testing.TB,
	matchingClient matchingservice.MatchingServiceClient,
	namespaceID namespace.ID,
	tv *testvars.TestVars,
	tqTypes ...enumspb.TaskQueueType,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now := timestamp.TimePtr(time.Now())
	resp, err := matchingClient.SyncDeploymentUserData(
		ctx, &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    namespaceID.String(),
			TaskQueue:      tv.TaskQueue().GetName(),
			TaskQueueTypes: tqTypes,
			Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
				UpdateVersionData: &deploymentspb.DeploymentVersionData{
					Version:           tv.DeploymentVersion(),
					RoutingUpdateTime: now,
					CurrentSinceTime:  now,
				},
			},
		},
	)
	require.NoError(t, err)

	// Wait for the data to propagate to all partitions.
	_, err = matchingClient.CheckTaskQueueUserDataPropagation(
		ctx, &matchingservice.CheckTaskQueueUserDataPropagationRequest{
			NamespaceId: namespaceID.String(),
			TaskQueue:   tv.TaskQueue().GetName(),
			Version:     resp.GetVersion(),
		},
	)
	require.NoError(t, err)
}
