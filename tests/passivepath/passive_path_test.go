package passivepath

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func echoActivity(_ context.Context, in string) (string, error) {
	return in, nil
}

// passivePathWorkflow first continues as new, then exercises three refresher
// branches: refreshWorkflowTaskTasks (every workflow task),
// refreshTasksForActivity (the activity), and refreshTasksForTimer (two concurrent
// timers, including creation of the later timer's task after the earlier one fires).
func passivePathWorkflow(ctx workflow.Context, continued bool) (string, error) {
	if !continued {
		return "", workflow.NewContinueAsNewError(ctx, passivePathWorkflow, true)
	}

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: 20 * time.Second,
	})

	var echoed string
	if err := workflow.ExecuteActivity(ctx, echoActivity, "hello").Get(ctx, &echoed); err != nil {
		return "", err
	}
	firstTimer := workflow.NewTimer(ctx, time.Second)
	secondTimer := workflow.NewTimer(ctx, 2*time.Second)
	if err := firstTimer.Get(ctx, nil); err != nil {
		return "", err
	}
	if err := secondTimer.Get(ctx, nil); err != nil {
		return "", err
	}
	return echoed, nil
}

// newSingleClusterWithGlobalNamespace starts one in-process cluster with global
// namespaces enabled.
//
// Global namespaces are required, not cosmetic: history_engine.go only constructs the
// entire NDC stack -- including nDCWorkflowStateReplicator, which owns
// ReplicateVersionedTransition -- when ClusterMetadata.IsGlobalNamespaceEnabled(). With
// the default local-namespace functional setup that field is nil and the apply
// nil-panics. That is also why testcore.NewEnv cannot be used here: it exposes no
// global-namespace option and registers namespaces with IsGlobalNamespace: false.
//
// Only one cluster is created. Nothing needs a remote peer: artifacts are produced and
// applied entirely within this cluster.
func newSingleClusterWithGlobalNamespace(t *testing.T, logger log.Logger) *testcore.TestCluster {
	clusterName := "passivepath_" + common.GenerateRandomString(5)

	persistenceDefaults := testcore.GetPersistenceTestDefaults()
	persistenceDefaults.DBName += "_" + clusterName

	config := &testcore.TestClusterConfig{
		ClusterMetadata: cluster.Config{
			EnableGlobalNamespace:    true,
			FailoverVersionIncrement: 10,
			MasterClusterName:        clusterName,
			CurrentClusterName:       clusterName,
			ClusterInformation: map[string]cluster.ClusterInformation{
				clusterName: {
					Enabled:                true,
					InitialFailoverVersion: 1,
				},
			},
		},
		HistoryConfig: testcore.HistoryConfig{NumHistoryShards: 1},
		Persistence:   persistenceDefaults,
		DynamicConfigOverrides: map[dynamicconfig.Key]any{
			// Artifacts are anchored on transition history; without it every write bails out.
			dynamicconfig.EnableTransitionHistory.Key():       true,
			dynamicconfig.NamespaceCacheRefreshInterval.Key(): testcore.NamespaceCacheRefreshInterval,
			// tests/xdc/base.go sets this for transition-history replication; the raw-history
			// path is what carries artifact event batches between internal services.
			dynamicconfig.SendRawHistoryBetweenInternalServices.Key(): true,
			// Keep queue processing brisk, so a missing task surfaces as a quick stall
			// rather than a slow test.
			dynamicconfig.TransferProcessorUpdateAckInterval.Key(): time.Second,
			dynamicconfig.TimerProcessorUpdateAckInterval.Key():    time.Second,
			dynamicconfig.TransferProcessorMaxPollInterval.Key():   time.Second,
			dynamicconfig.TimerProcessorMaxPollInterval.Key():      time.Second,
		},
		EnableHistoryTaskRecorder: true,
	}

	tc, err := testcore.NewTestClusterFactory().NewCluster(t, config, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tc.TearDownCluster() })
	return tc
}

func registerGlobalNamespace(t *testing.T, tc *testcore.TestCluster, name string) namespace.ID {
	clusterName := tc.ClusterName()
	_, err := tc.FrontendClient().RegisterNamespace(
		testcore.NewContext(),
		&workflowservice.RegisterNamespaceRequest{
			Namespace:                        name,
			IsGlobalNamespace:                true,
			ActiveClusterName:                clusterName,
			Clusters:                         []*replicationpb.ClusterReplicationConfig{{ClusterName: clusterName}},
			WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		})
	require.NoError(t, err)

	describeResponse, err := tc.FrontendClient().DescribeNamespace(
		testcore.NewContext(),
		&workflowservice.DescribeNamespaceRequest{Namespace: name},
	)
	require.NoError(t, err)
	namespaceID := namespace.ID(describeResponse.GetNamespaceInfo().GetId())

	// Let the namespace registry pick it up before any workflow starts.
	time.Sleep(2 * testcore.NamespaceCacheRefreshInterval) //nolint:forbidigo

	searchAttributes := searchattribute.TestSearchAttributesToRegister()
	searchAttributes["SimulatedFailure"] = enumspb.INDEXED_VALUE_TYPE_BOOL
	_, err = tc.OperatorClient().AddSearchAttributes(
		testcore.NewContext(),
		&operatorservice.AddSearchAttributesRequest{
			Namespace:        name,
			SearchAttributes: searchAttributes,
		},
	)
	require.NoError(t, err)
	return namespaceID
}

// TestPassivePath_WorkflowRunsOnRefresherGeneratedTasks is the primary oracle. Every
// active commit is diverted through ReplicateVersionedTransition, so the only tasks in
// the DB are the ones workflow.TaskRefresher produced. If the refresher fails to
// generate a task the workflow needed, this test does not fail an assertion -- it
// stalls and times out. That stall *is* the signal.
func TestPassivePath_WorkflowRunsOnRefresherGeneratedTasks(t *testing.T) {
	// Sticky execution must be off. When the workflow task response carries the next
	// task inline, RespondWorkflowTaskCompleted sets bypassTaskGeneration and no transfer
	// task is written at all -- so the workflow would advance without ever consuming a
	// refresher-generated task, hiding exactly the bugs under test.
	sdkworker.SetStickyWorkflowCacheSize(0)

	logger := log.NewTestLogger()
	harness := NewHarness(logger)

	tc := newSingleClusterWithGlobalNamespace(t, logger)

	t.Cleanup(func() {
		t.Logf("PASSIVEPATH intercepted=%d diverted=%d applied=%d "+
			"standbyExecutions=%d bailouts=%v allBailouts=%v applyErrs=%v",
			harness.Intercepted(), harness.Diverted(), harness.Applied(),
			harness.StandbyExecutions(),
			harness.Bailouts(), harness.AllBailouts(), harness.ApplyErrors())
	})

	ns := "passive-path-ns"
	namespaceID := registerGlobalNamespace(t, tc, ns)
	t.Cleanup(tc.InjectHook(t, testhooks.NewHook[testhooks.HistoryPassiveReplicationTestHook](
		testhooks.HistoryPassiveReplicationTest,
		harness,
	), namespaceID))

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  tc.Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	require.NoError(t, err)
	t.Cleanup(sdkClient.Close)

	taskQueue := "passive-path-tq"
	w := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
	w.RegisterWorkflow(passivePathWorkflow)
	w.RegisterActivity(echoActivity)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                 "passive-path-workflow",
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: 60 * time.Second,
	}, passivePathWorkflow, false)
	require.NoError(t, err)

	var result string
	require.NoError(t, run.Get(ctx, &result), "workflow did not complete: the refresher "+
		"likely failed to generate a task the workflow needed")
	require.Equal(t, "hello", result)

	// The harness must not have quietly fallen back to active writes. A bail-out commits
	// active-generated tasks, which masks the refresher gap under test, so any bail-out in
	// this scenario invalidates the run rather than merely reducing it.
	require.Empty(t, harness.Bailouts(), "writes bailed out of the passive path")
	require.Empty(t, harness.ApplyErrors(), "artifact apply failed")
	require.Positive(t, harness.Diverted(), "no write was diverted; the harness tested nothing")
	require.Equal(t, harness.Diverted(), harness.Applied(),
		"some diverted writes were never applied")
	require.Positive(t, harness.StandbyExecutions(), "no task was exercised in standby mode")
}
