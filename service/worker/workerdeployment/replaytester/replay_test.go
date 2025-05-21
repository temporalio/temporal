package replaytester

// TestReplays tests workflow logic backwards compatibility from previous versions.
//func TestReplays(t *testing.T) {
//	replayer := worker.NewWorkflowReplayer()
//	drainageWorkflow := func(ctx workflow.Context, args *deploymentspb.DrainageWorkflowArgs) error {
//		refreshIntervalGetter := func() any {
//			return time.Minute
//		}
//		visibilityGracePeriodGetter := func() any {
//			return time.Minute
//		}
//		return workerdeployment.DrainageWorkflow(ctx, refreshIntervalGetter, visibilityGracePeriodGetter, args)
//	}
//	deploymentWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
//		maxVersionsGetter := func() int {
//			return 100
//		}
//		return workerdeployment.Workflow(ctx, maxVersionsGetter, args)
//	}
//	replayer.RegisterWorkflowWithOptions(workerdeployment.VersionWorkflow, workflow.RegisterOptions{Name: workerdeployment.WorkerDeploymentVersionWorkflowType})
//	replayer.RegisterWorkflowWithOptions(deploymentWorkflow, workflow.RegisterOptions{Name: workerdeployment.WorkerDeploymentWorkflowType})
//	replayer.RegisterWorkflowWithOptions(drainageWorkflow, workflow.RegisterOptions{Name: workerdeployment.WorkerDeploymentDrainageWorkflowType})
//
//	files, err := filepath.Glob("testdata/replay_*.json.gz")
//	require.NoError(t, err)
//
//	fmt.Println("Number of files to replay:", len(files))
//
//	logger := log.NewSdkLogger(log.NewTestLogger())
//
//	for _, filename := range files {
//		logger.Info("Replaying", "file", filename)
//		f, err := os.Open(filename)
//		require.NoError(t, err)
//		r, err := gzip.NewReader(f)
//		require.NoError(t, err)
//		history, err := client.HistoryFromJSON(r, client.HistoryJSONOptions{})
//		require.NoError(t, err)
//		err = replayer.ReplayWorkflowHistory(logger, history)
//		require.NoError(t, err)
//		_ = r.Close()
//		_ = f.Close()
//	}
//}
