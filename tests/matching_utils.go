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
func runWithMatchingBehaviors(
	t *testing.T,
	baseOpts []testcore.TestOption,
	subtest func(s *testcore.TestEnv, behavior testcore.MatchingBehavior),
) {
	for _, behavior := range testcore.AllMatchingBehaviors() {
		t.Run(behavior.Name(), func(t *testing.T) {
			opts := append([]testcore.TestOption{}, baseOpts...)
			opts = append(opts, behavior.Options()...)

			env := testcore.NewEnv(t, opts...)
			behavior.InjectHooks(env)

			subtest(env, behavior)
		})
	}
}

//
//func runWithMatchingBehaviors(t *testing.T, subtest func(s *testcore.TestEnv, behavior testcore.MatchingBehavior)) {
//	type testCase struct {
//		name             string
//		forcePollForward bool
//		forceTaskForward bool
//		forceAsync       bool
//	}
//
//	var cases []testCase
//	for _, forcePollForward := range []bool{false, true} {
//		for _, forceTaskForward := range []bool{false, true} {
//			for _, forceAsync := range []bool{false, true} {
//				name := "NoTaskForward"
//				if forceTaskForward {
//					name = "ForceTaskForward"
//				}
//				if forcePollForward {
//					name += "ForcePollForward"
//				} else {
//					name += "NoPollForward"
//				}
//				if forceAsync {
//					name += "ForceAsync"
//				} else {
//					name += "AllowSync"
//				}
//				cases = append(cases, testCase{
//					name:             name,
//					forcePollForward: forcePollForward,
//					forceTaskForward: forceTaskForward,
//					forceAsync:       forceAsync,
//				})
//			}
//		}
//	}
//
//	for _, tc := range cases {
//		t.Run(tc.name, func(t *testing.T) {
//			// TODO: We can avoid separte cluster by take task queue name as input and only override config for that queue.
//			// In that case we need to also make hooks per queue.
//			s := testcore.NewEnv(t, testcore.WithDedicatedCluster())
//
//			if tc.forceTaskForward || tc.forcePollForward {
//				s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 13)
//				s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 13)
//			} else {
//				s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1)
//				s.OverrideDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1)
//			}
//
//			writePartition := 0
//			if tc.forceTaskForward {
//				writePartition = 11
//			}
//			s.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceWritePartition, writePartition))
//
//			readPartition := 0
//			if tc.forcePollForward {
//				readPartition = 5
//			}
//			s.InjectHook(testhooks.NewHook(testhooks.MatchingLBForceReadPartition, readPartition))
//			s.InjectHook(testhooks.NewHook(testhooks.MatchingDisableSyncMatch, tc.forceAsync))
//
//			subtest(s, tc.forcePollForward, tc.forceTaskForward, tc.forceAsync)
//		})
//	}
//}

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
