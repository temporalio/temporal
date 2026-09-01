package workerdeployment

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/worker_versioning"
	"go.uber.org/mock/gomock"
)

func TestRegistrationErrorCacheScopes(t *testing.T) {
	t.Parallel()

	type cacheLookup struct {
		namespaceID    string
		deploymentName string
		buildID        string
	}
	tests := []struct {
		name       string
		errorType  string
		cacheError func(*ClientImpl, error)
		hits       []cacheLookup
		misses     []cacheLookup
	}{
		{
			name:      "too many versions",
			errorType: errTooManyVersions,
			cacheError: func(client *ClientImpl, err error) {
				client.cacheTooManyVersionsError(
					"namespace-a", "deployment-a", err, versionFingerprintSet("deployment-a", "build-existing"),
				)
			},
			hits: []cacheLookup{
				{namespaceID: "namespace-a", deploymentName: "deployment-a", buildID: "build-b"},
			},
			misses: []cacheLookup{
				{namespaceID: "namespace-a", deploymentName: "deployment-a", buildID: "build-existing"},
				{namespaceID: "namespace-a", deploymentName: "deployment-b", buildID: "build-a"},
				{namespaceID: "namespace-b", deploymentName: "deployment-a", buildID: "build-a"},
			},
		},
		{
			name:      "too many task queues",
			errorType: errMaxTaskQueuesInVersionType,
			cacheError: func(client *ClientImpl, err error) {
				client.cacheMaxTaskQueuesInVersionError("namespace-a", "deployment-a", "build-a", err)
			},
			hits: []cacheLookup{
				{namespaceID: "namespace-a", deploymentName: "deployment-a", buildID: "build-a"},
			},
			misses: []cacheLookup{
				{namespaceID: "namespace-a", deploymentName: "deployment-a", buildID: "build-b"},
				{namespaceID: "namespace-a", deploymentName: "deployment-b", buildID: "build-a"},
				{namespaceID: "namespace-b", deploymentName: "deployment-a", buildID: "build-a"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			timeSource := clock.NewEventTimeSource()
			errorCache := cache.New(100, &cache.Options{TTL: time.Minute, TimeSource: timeSource})
			t.Cleanup(errorCache.Stop)
			client := &ClientImpl{
				registrationErrorCache:                    errorCache,
				workerDeploymentRegistrationErrorCacheTTL: time.Minute,
				registrationErrorCacheTimeSource:          timeSource,
			}
			cachedErr := errors.New(test.name)

			test.cacheError(client, cachedErr)

			for _, key := range test.hits {
				actualErrorType, actualErr := client.getCachedTooManyVersionsOrTaskQueuesError(key.namespaceID, key.deploymentName, key.buildID)
				if test.errorType == errTooManyVersions {
					require.EqualError(t, actualErr, "too many versions; this cached result may not reflect recent version changes, retry in up to 1m0s")
					var resourceExhaustedErr *serviceerror.ResourceExhausted
					require.ErrorAs(t, actualErr, &resourceExhaustedErr)
					require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS, resourceExhaustedErr.Cause)
				} else {
					require.ErrorIs(t, actualErr, cachedErr)
				}
				require.Equal(t, test.errorType, actualErrorType)
			}
			for _, key := range test.misses {
				actualErrorType, actualErr := client.getCachedTooManyVersionsOrTaskQueuesError(key.namespaceID, key.deploymentName, key.buildID)
				require.NoError(t, actualErr)
				require.Empty(t, actualErrorType)
			}
		})
	}
}

func TestTooManyDeploymentsErrorCacheScope(t *testing.T) {
	t.Parallel()

	errorCache := cache.New(100, &cache.Options{TTL: time.Minute})
	t.Cleanup(errorCache.Stop)
	client := &ClientImpl{registrationErrorCache: errorCache}
	cachedErr := errors.New("too many deployments")

	client.cacheTooManyDeploymentsError("namespace-a", cachedErr)

	actualCachedErr := client.getCachedTooManyDeploymentsError("namespace-a")
	require.NotNil(t, actualCachedErr)
	require.ErrorIs(t, actualCachedErr.err, cachedErr)
	require.Nil(t, client.getCachedTooManyDeploymentsError("namespace-b"))
	errorType, actualErr := client.getCachedTooManyVersionsOrTaskQueuesError("namespace-a", "deployment-a", "build-a")
	require.NoError(t, actualErr)
	require.Empty(t, errorType)
}

func TestCheckWorkerDeploymentLimitCachesDetectedError(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	ns, _ := createMockNamespaceCache(controller, namespace.Name("namespace-a"))
	visibilityManager := manager.NewMockVisibilityManager(controller)
	timeSource := clock.NewEventTimeSource()
	cacheTTL := 10 * time.Second
	errorCache := cache.New(100, &cache.Options{
		TTL:        cacheTTL,
		TimeSource: timeSource,
	})
	t.Cleanup(errorCache.Stop)
	client := &ClientImpl{
		visibilityManager:                         visibilityManager,
		maxDeployments:                            dynamicconfig.GetIntPropertyFnFilteredByNamespace(1),
		metricsHandler:                            metrics.NoopMetricsHandler,
		registrationErrorCache:                    errorCache,
		workerDeploymentRegistrationErrorCacheTTL: cacheTTL,
		registrationErrorCacheTimeSource:          timeSource,
	}
	visibilityManager.EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).Return(
		&manager.CountWorkflowExecutionsResponse{Count: 1},
		nil,
	)

	cacheHit, errorType, err := client.checkWorkerDeploymentLimit(context.Background(), ns)

	require.False(t, cacheHit)
	require.Equal(t, errTooManyDeployments, errorType)
	require.EqualError(t, err, "reached maximum deployments in namespace (1)")
	timeSource.Advance(3 * time.Second)

	cacheHit, errorType, err = client.checkWorkerDeploymentLimit(context.Background(), ns)

	require.True(t, cacheHit)
	require.Equal(t, errTooManyDeployments, errorType)
	require.EqualError(
		t,
		err,
		"reached maximum deployments in namespace (1); this cached result may not reflect a recent deletion, retry in up to 7s",
	)
}

func TestUpdateWithStartWorkerDeploymentChecksDeploymentLimitCacheOnlyForNewDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		deploymentIsRunning bool
		wantCacheHit        bool
		wantErrorType       string
	}{
		{
			name:          "new deployment uses cached error",
			wantCacheHit:  true,
			wantErrorType: errTooManyDeployments,
		},
		{
			name:                "existing deployment bypasses cached error",
			deploymentIsRunning: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			controller := gomock.NewController(t)
			ns, _ := createMockNamespaceCache(controller, namespace.Name("namespace-a"))
			historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
			timeSource := clock.NewEventTimeSource()
			cacheTTL := 10 * time.Second
			errorCache := cache.New(100, &cache.Options{
				TTL:        cacheTTL,
				TimeSource: timeSource,
			})
			t.Cleanup(errorCache.Stop)
			client := &ClientImpl{
				historyClient:                             historyClient,
				maxIDLengthLimit:                          dynamicconfig.GetIntPropertyFn(testMaxIDLengthLimit),
				registrationErrorCache:                    errorCache,
				workerDeploymentRegistrationErrorCacheTTL: cacheTTL,
				registrationErrorCacheTimeSource:          timeSource,
			}
			cachedErr := newResourceExhaustedError("reached maximum deployments in namespace (100)")
			client.cacheTooManyDeploymentsError(ns.ID().String(), cachedErr)
			timeSource.Advance(3 * time.Second)

			if test.deploymentIsRunning {
				historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
					&historyservice.DescribeWorkflowExecutionResponse{
						WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
							Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
						},
					},
					nil,
				)
				rpcErr := errors.New("execute multi operation")
				historyClient.EXPECT().ExecuteMultiOperation(gomock.Any(), gomock.Any()).Return(nil, rpcErr)
				_, cacheHit, errorType, err := client.updateWithStartWorkerDeployment(
					context.Background(), ns, "deployment-a", &updatepb.Request{}, "identity", "request-id", 1,
				)
				require.ErrorIs(t, err, rpcErr)
				require.False(t, cacheHit)
				require.Empty(t, errorType)
				return
			}

			historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("not found"))
			_, cacheHit, errorType, err := client.updateWithStartWorkerDeployment(
				context.Background(), ns, "deployment-a", &updatepb.Request{}, "identity", "request-id", 1,
			)
			require.EqualError(t, err, "reached maximum deployments in namespace (100); this cached result may not reflect a recent deletion, retry in up to 7s")
			var resourceExhaustedErr *serviceerror.ResourceExhausted
			require.ErrorAs(t, err, &resourceExhaustedErr)
			require.Equal(t, enumspb.RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS, resourceExhaustedErr.Cause)
			require.Equal(t, test.wantCacheHit, cacheHit)
			require.Equal(t, test.wantErrorType, errorType)
		})
	}
}

func TestAllowNoPollersPathsRecordAndCacheTooManyVersionsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		operation string
		call      func(*ClientImpl, *namespace.Namespace, string) error
	}{
		{
			operation: "SetCurrentVersion",
			call: func(client *ClientImpl, ns *namespace.Namespace, version string) error {
				_, err := client.SetCurrentVersion(
					context.Background(), ns, "deployment-a", version, "identity", false, nil, true,
				)
				return err
			},
		},
		{
			operation: "SetRampingVersion",
			call: func(client *ClientImpl, ns *namespace.Namespace, version string) error {
				_, err := client.SetRampingVersion(
					context.Background(), ns, "deployment-a", version, 10, "identity", false, nil, true,
				)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.operation, func(t *testing.T) {
			t.Parallel()

			controller := gomock.NewController(t)
			ns, _ := createMockNamespaceCache(controller, namespace.Name("namespace-a"))
			historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
			timeSource := clock.NewEventTimeSource()
			cacheTTL := 10 * time.Second
			errorCache := cache.New(100, &cache.Options{TTL: cacheTTL, TimeSource: timeSource})
			t.Cleanup(errorCache.Stop)
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			t.Cleanup(func() { metricsHandler.StopCapture(capture) })
			client := &ClientImpl{
				logger:                 log.NewNoopLogger(),
				historyClient:          historyClient,
				maxIDLengthLimit:       dynamicconfig.GetIntPropertyFn(testMaxIDLengthLimit),
				metricsHandler:         metricsHandler,
				registrationErrorCache: errorCache,
				workerDeploymentRegistrationErrorCacheTTL: cacheTTL,
				registrationErrorCacheTimeSource:          timeSource,
			}
			version := worker_versioning.WorkerDeploymentVersionToStringV31(&deploymentspb.WorkerDeploymentVersion{
				DeploymentName: "deployment-a",
				BuildId:        "build-new",
			})
			details, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.TooManyVersionsFailureDetails{
				VersionFingerprints: []uint64{workerDeploymentVersionFingerprint("deployment-a", "build-existing")},
			})
			require.NoError(t, err)
			failure := &failurepb.Failure{
				Message: "reached maximum versions in deployment (100)",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						Type:    errTooManyVersions,
						Details: details,
					},
				},
			}
			historyClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
				&historyservice.DescribeWorkflowExecutionResponse{
					WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
						Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					},
				},
				nil,
			)
			historyClient.EXPECT().ExecuteMultiOperation(gomock.Any(), gomock.Any()).Return(
				&historyservice.ExecuteMultiOperationResponse{
					Responses: []*historyservice.ExecuteMultiOperationResponse_Response{
						{
							Response: &historyservice.ExecuteMultiOperationResponse_Response_StartWorkflow{
								StartWorkflow: &historyservice.StartWorkflowExecutionResponse{},
							},
						},
						{
							Response: &historyservice.ExecuteMultiOperationResponse_Response_UpdateWorkflow{
								UpdateWorkflow: &historyservice.UpdateWorkflowExecutionResponse{
									Response: &workflowservice.UpdateWorkflowExecutionResponse{
										Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
										Outcome: &updatepb.Outcome{Value: &updatepb.Outcome_Failure{
											Failure: failure,
										}},
									},
								},
							},
						},
					},
				},
				nil,
			)

			err = test.call(client, ns, version)

			require.EqualError(t, err, "reached maximum versions in deployment (100)")
			require.NotNil(t, client.getCachedTooManyVersionsError(ns.ID().String(), "deployment-a", "build-new"))
			require.Nil(t, client.getCachedTooManyVersionsError(ns.ID().String(), "deployment-a", "build-existing"))
			recordings := capture.Snapshot()[metrics.WorkerDeploymentRegistrationErrors.Name()]
			require.Len(t, recordings, 1)
			require.Equal(t, map[string]string{
				"namespace":              "namespace-a",
				metrics.OperationTagName: test.operation,
				metrics.ErrorTypeTagName: errTooManyVersions,
				metrics.CacheHitTagName:  "false",
			}, recordings[0].Tags)
		})
	}
}

func TestCreateWorkerDeploymentChecksCachedDeploymentLimitBeforeCounting(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	ns, _ := createMockNamespaceCache(controller, namespace.Name("namespace-a"))
	historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
	timeSource := clock.NewEventTimeSource()
	cacheTTL := 10 * time.Second
	errorCache := cache.New(100, &cache.Options{
		TTL:        cacheTTL,
		TimeSource: timeSource,
	})
	t.Cleanup(errorCache.Stop)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	client := &ClientImpl{
		logger:                 log.NewNoopLogger(),
		historyClient:          historyClient,
		maxIDLengthLimit:       dynamicconfig.GetIntPropertyFn(testMaxIDLengthLimit),
		metricsHandler:         metricsHandler,
		registrationErrorCache: errorCache,
		workerDeploymentRegistrationErrorCacheTTL: cacheTTL,
		registrationErrorCacheTimeSource:          timeSource,
	}
	cachedErr := newResourceExhaustedError("reached maximum deployments in namespace (100)")
	client.cacheTooManyDeploymentsError(ns.ID().String(), cachedErr)
	timeSource.Advance(3 * time.Second)
	historyClient.EXPECT().QueryWorkflow(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("not found"))

	_, err := client.CreateWorkerDeployment(context.Background(), ns, "deployment-a", "identity", "request-id")

	require.EqualError(
		t,
		err,
		"reached maximum deployments in namespace (100); this cached result may not reflect a recent deletion, retry in up to 7s",
	)
	recordings := capture.Snapshot()[metrics.WorkerDeploymentRegistrationErrors.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, map[string]string{
		"namespace":              "namespace-a",
		metrics.OperationTagName: "CreateWorkerDeployment",
		metrics.ErrorTypeTagName: errTooManyDeployments,
		metrics.CacheHitTagName:  "true",
	}, recordings[0].Tags)
}

func TestCreateWorkerDeploymentVersionCachesVersionLimitError(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	ns, _ := createMockNamespaceCache(controller, namespace.Name("namespace-a"))
	historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
	timeSource := clock.NewEventTimeSource()
	cacheTTL := 10 * time.Second
	errorCache := cache.New(100, &cache.Options{TTL: cacheTTL, TimeSource: timeSource})
	t.Cleanup(errorCache.Stop)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	client := &ClientImpl{
		logger:                 log.NewNoopLogger(),
		historyClient:          historyClient,
		maxIDLengthLimit:       dynamicconfig.GetIntPropertyFn(testMaxIDLengthLimit),
		metricsHandler:         metricsHandler,
		registrationErrorCache: errorCache,
		workerDeploymentRegistrationErrorCacheTTL: cacheTTL,
		registrationErrorCacheTimeSource:          timeSource,
	}
	details, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.TooManyVersionsFailureDetails{
		VersionFingerprints: []uint64{workerDeploymentVersionFingerprint("deployment-a", "build-existing")},
	})
	require.NoError(t, err)
	failure := &failurepb.Failure{
		Message: "reached maximum versions in deployment (100)",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:    errTooManyVersions,
				Details: details,
			},
		},
	}
	historyClient.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&historyservice.UpdateWorkflowExecutionResponse{
			Response: &workflowservice.UpdateWorkflowExecutionResponse{
				Stage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
				Outcome: &updatepb.Outcome{Value: &updatepb.Outcome_Failure{
					Failure: failure,
				}},
			},
		},
		nil,
	)

	err = client.CreateWorkerDeploymentVersion(
		context.Background(), ns, "deployment-a", "build-new", "identity", "request-id", nil,
	)

	require.EqualError(t, err, "reached maximum versions in deployment (100)")
	require.NotNil(t, client.getCachedTooManyVersionsError(ns.ID().String(), "deployment-a", "build-new"))
	require.Nil(t, client.getCachedTooManyVersionsError(ns.ID().String(), "deployment-a", "build-existing"))
	recordings := capture.Snapshot()[metrics.WorkerDeploymentRegistrationErrors.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, map[string]string{
		"namespace":              "namespace-a",
		metrics.OperationTagName: "CreateWorkerDeploymentVersion",
		metrics.ErrorTypeTagName: errTooManyVersions,
		metrics.CacheHitTagName:  "false",
	}, recordings[0].Tags)
}

func TestCreateWorkerDeploymentVersionChecksCachedVersionLimit(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	ns, _ := createMockNamespaceCache(controller, namespace.Name("namespace-a"))
	timeSource := clock.NewEventTimeSource()
	cacheTTL := 10 * time.Second
	errorCache := cache.New(100, &cache.Options{TTL: cacheTTL, TimeSource: timeSource})
	t.Cleanup(errorCache.Stop)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	client := &ClientImpl{
		logger:                 log.NewNoopLogger(),
		maxIDLengthLimit:       dynamicconfig.GetIntPropertyFn(testMaxIDLengthLimit),
		metricsHandler:         metricsHandler,
		registrationErrorCache: errorCache,
		workerDeploymentRegistrationErrorCacheTTL: cacheTTL,
		registrationErrorCacheTimeSource:          timeSource,
	}
	client.cacheTooManyVersionsError(
		ns.ID().String(),
		"deployment-a",
		newResourceExhaustedError("reached maximum versions in deployment (100)"),
		versionFingerprintSet("deployment-a", "build-existing"),
	)
	timeSource.Advance(3 * time.Second)

	err := client.CreateWorkerDeploymentVersion(
		context.Background(), ns, "deployment-a", "build-new", "identity", "request-id", nil,
	)

	require.EqualError(
		t,
		err,
		"reached maximum versions in deployment (100); this cached result may not reflect recent version changes, retry in up to 7s",
	)
	recordings := capture.Snapshot()[metrics.WorkerDeploymentRegistrationErrors.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, map[string]string{
		"namespace":              "namespace-a",
		metrics.OperationTagName: "CreateWorkerDeploymentVersion",
		metrics.ErrorTypeTagName: errTooManyVersions,
		metrics.CacheHitTagName:  "true",
	}, recordings[0].Tags)
}

func TestCreateWorkerDeploymentVersionBypassesCachedVersionLimitForExistingVersion(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	ns, _ := createMockNamespaceCache(controller, namespace.Name("namespace-a"))
	historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
	errorCache := cache.New(100, &cache.Options{TTL: time.Minute})
	t.Cleanup(errorCache.Stop)
	client := &ClientImpl{
		logger:                 log.NewNoopLogger(),
		historyClient:          historyClient,
		maxIDLengthLimit:       dynamicconfig.GetIntPropertyFn(testMaxIDLengthLimit),
		metricsHandler:         metrics.NoopMetricsHandler,
		registrationErrorCache: errorCache,
	}
	client.cacheTooManyVersionsError(
		ns.ID().String(),
		"deployment-a",
		newResourceExhaustedError("reached maximum versions in deployment (100)"),
		versionFingerprintSet("deployment-a", "build-existing"),
	)
	historyClient.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&historyservice.UpdateWorkflowExecutionResponse{
			Response: &workflowservice.UpdateWorkflowExecutionResponse{
				Stage:   enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
				Outcome: &updatepb.Outcome{Value: &updatepb.Outcome_Success{Success: &commonpb.Payloads{}}},
			},
		},
		nil,
	)

	err := client.CreateWorkerDeploymentVersion(
		context.Background(), ns, "deployment-a", "build-existing", "identity", "request-id", nil,
	)

	require.NoError(t, err)
}

func TestRegistrationErrorCacheOnlyCachesErrorsUntilTTL(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	ttl := 10 * time.Second
	errorCache := cache.New(100, &cache.Options{
		TTL:        ttl,
		TimeSource: timeSource,
	})
	t.Cleanup(errorCache.Stop)
	client := &ClientImpl{
		registrationErrorCache:                    errorCache,
		workerDeploymentRegistrationErrorCacheTTL: ttl,
		registrationErrorCacheTimeSource:          timeSource,
	}

	cachedErr := errors.New("cached error")
	client.cacheTooManyVersionsError("namespace-a", "deployment-a", nil, nil)
	require.Zero(t, errorCache.Size())

	client.cacheTooManyVersionsError("namespace-a", "deployment-a", cachedErr, nil)
	require.Zero(t, errorCache.Size())

	client.cacheTooManyVersionsError(
		"namespace-a",
		"deployment-a",
		cachedErr,
		versionFingerprintSet("deployment-a", "build-existing"),
	)
	_, actualErr := client.getCachedTooManyVersionsOrTaskQueuesError("namespace-a", "deployment-a", "build-b")
	require.Error(t, actualErr)
	_, actualErr = client.getCachedTooManyVersionsOrTaskQueuesError("namespace-a", "deployment-a", "build-existing")
	require.NoError(t, actualErr)

	timeSource.Advance(ttl + time.Nanosecond)
	_, actualErr = client.getCachedTooManyVersionsOrTaskQueuesError("namespace-a", "deployment-a", "build-b")
	require.NoError(t, actualErr)
}

func TestGetVersionFingerprintsFromFailure(t *testing.T) {
	t.Parallel()

	payloads, err := sdk.PreferProtoDataConverter.ToPayloads(&deploymentspb.TooManyVersionsFailureDetails{
		VersionFingerprints: []uint64{11, 22},
	})
	require.NoError(t, err)
	client := &ClientImpl{}

	fingerprints, err := client.getVersionFingerprintsFromFailure(&failurepb.Failure{
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Details: payloads},
		},
	})

	require.NoError(t, err)
	require.Equal(t, map[uint64]struct{}{11: {}, 22: {}}, fingerprints)

	fingerprints, err = client.getVersionFingerprintsFromFailure(&failurepb.Failure{
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{},
		},
	})
	require.NoError(t, err)
	require.Nil(t, fingerprints)
}

func versionFingerprintSet(deploymentName string, buildIDs ...string) map[uint64]struct{} {
	fingerprints := make(map[uint64]struct{}, len(buildIDs))
	for _, buildID := range buildIDs {
		fingerprints[workerDeploymentVersionFingerprint(deploymentName, buildID)] = struct{}{}
	}
	return fingerprints
}

func TestRecordWorkerDeploymentRegistrationError(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	client := &ClientImpl{metricsHandler: metricsHandler}
	err := errors.New("registration failed")

	client.recordRegistrationError("RegisterTaskQueueWorker", "namespace-a", errTooManyVersions, err, false)
	client.recordRegistrationError("RegisterTaskQueueWorker", "namespace-a", errTooManyVersions, err, true)

	recordings := capture.Snapshot()[metrics.WorkerDeploymentRegistrationErrors.Name()]
	require.Len(t, recordings, 2)
	require.Equal(t, int64(1), recordings[0].Value)
	require.Equal(t, map[string]string{
		"namespace":              "namespace-a",
		metrics.OperationTagName: "RegisterTaskQueueWorker",
		metrics.ErrorTypeTagName: errTooManyVersions,
		metrics.CacheHitTagName:  "false",
	}, recordings[0].Tags)
	require.Equal(t, int64(1), recordings[1].Value)
	require.Equal(t, map[string]string{
		"namespace":              "namespace-a",
		metrics.OperationTagName: "RegisterTaskQueueWorker",
		metrics.ErrorTypeTagName: errTooManyVersions,
		metrics.CacheHitTagName:  "true",
	}, recordings[1].Tags)
}
