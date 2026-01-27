package workerdeployment

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/consts"
	update2 "go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

// testMaxIDLengthLimit is the current default value used by dynamic config for
// MaxIDLengthLimit
const (
	testNamespace        = "deployment-test"
	testDeployment       = "A"
	testBuildID          = "xyz"
	testMaxIDLengthLimit = 1000
)

type (
	deploymentWorkflowClientSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		ns                 *namespace.Namespace
		mockNamespaceCache *namespace.MockRegistry
		mockHistoryClient  *historyservicemock.MockHistoryServiceClient
		VisibilityManager  *manager.MockVisibilityManager
		workerDeployment   *deploymentpb.Deployment
		deploymentClient   *ClientImpl
		sync.Mutex
	}
)

func (d *deploymentWorkflowClientSuite) SetupSuite() {
}

func (d *deploymentWorkflowClientSuite) TearDownSuite() {
}

func (d *deploymentWorkflowClientSuite) SetupTest() {
	d.Assertions = require.New(d.T())
	d.Lock()
	defer d.Unlock()
	d.controller = gomock.NewController(d.T())
	d.controller = gomock.NewController(d.T())
	d.ns, d.mockNamespaceCache = createMockNamespaceCache(d.controller, testNamespace)
	d.VisibilityManager = manager.NewMockVisibilityManager(d.controller)
	d.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(d.controller)
	d.workerDeployment = &deploymentpb.Deployment{
		SeriesName: testDeployment,
		BuildId:    testBuildID,
	}
	d.deploymentClient = &ClientImpl{
		historyClient:     d.mockHistoryClient,
		visibilityManager: d.VisibilityManager,
	}

}

func createMockNamespaceCache(controller *gomock.Controller, nsName namespace.Name) (*namespace.Namespace, *namespace.MockRegistry) {
	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: nsName.String()}, nil, "")
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(ns.Name(), nil).AnyTimes()
	return ns, mockNamespaceCache
}

func TestDeploymentWorkflowClientSuite(t *testing.T) {
	d := new(deploymentWorkflowClientSuite)
	suite.Run(t, d)
}

func (d *deploymentWorkflowClientSuite) TestValidateVersionWfParams() {
	testCases := []struct {
		Description   string
		FieldName     string
		Input         string
		ExpectedError error
	}{
		{
			Description:   "Empty Field",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         "",
			ExpectedError: serviceerror.NewInvalidArgument("WorkerDeploymentName cannot be empty"),
		},
		{
			Description:   "Large Field",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         strings.Repeat("s", 1000),
			ExpectedError: serviceerror.NewInvalidArgument("size of WorkerDeploymentName larger than the maximum allowed"),
		},
		{
			Description:   "Valid field",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         "A",
			ExpectedError: nil,
		},
		{
			Description:   "Invalid buildID",
			FieldName:     worker_versioning.WorkerDeploymentBuildIDFieldName,
			Input:         "__unversioned__",
			ExpectedError: serviceerror.NewInvalidArgument("BuildID cannot start with '__'"),
		},
		{
			Description:   "Invalid buildID",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         "__my_dep",
			ExpectedError: serviceerror.NewInvalidArgument("WorkerDeploymentName cannot start with '__'"),
		},
		{
			Description:   "Valid buildID",
			FieldName:     worker_versioning.WorkerDeploymentBuildIDFieldName,
			Input:         "valid_build__id",
			ExpectedError: nil,
		},
		{
			Description:   "Invalid deploymentName",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         "A/B",
			ExpectedError: nil,
		},
		{
			Description:   "Invalid deploymentName",
			FieldName:     worker_versioning.WorkerDeploymentNameFieldName,
			Input:         "A.B",
			ExpectedError: serviceerror.NewInvalidArgument("worker deployment name cannot contain '.'"),
		},
	}

	for _, test := range testCases {
		fieldName := test.FieldName
		field := test.Input
		err := validateVersionWfParams(fieldName, field, testMaxIDLengthLimit)

		if test.ExpectedError == nil {
			d.NoError(err)
			continue
		}

		var invalidArgument *serviceerror.InvalidArgument
		d.ErrorAs(err, &invalidArgument)
		d.Equal(test.ExpectedError.Error(), err.Error())
	}
}

func TestDecodeWorkerDeploymentMemoTolerateUnknownFields(t *testing.T) {
	t.Parallel()
	decodeAndValidateMemo(t, "testdata/memo_with_last_current_time.json", "test-deployment", "build-1")
}

func TestDecodeWorkerDeploymentMemoTolerateMissingFields(t *testing.T) {
	t.Parallel()
	decodeAndValidateMemo(t, "testdata/memo_without_last_current_time.json", "test-deployment", "build-1")
}

func decodeAndValidateMemo(t *testing.T, filePath, deploymentName, buildID string) {
	jsonData, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// Create a payload with json/protobuf encoding (matching SDK's ProtoJSONPayloadConverter)
	payload := &commonpb.Payload{
		Metadata: map[string][]byte{
			"encoding":    []byte("json/protobuf"),
			"messageType": []byte("temporal.server.api.deployment.v1.WorkerDeploymentWorkflowMemo"),
		},
		Data: jsonData,
	}

	// Create a memo with the payload
	memo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			WorkerDeploymentMemoField: payload,
		},
	}

	// Decode should succeed even if the payload contains unknown fields
	result, err := DecodeWorkerDeploymentMemo(memo)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify known fields are decoded correctly
	require.Equal(t, deploymentName, result.DeploymentName)
	require.NotNil(t, result.CreateTime)
	require.NotNil(t, result.RoutingConfig)
	require.Equal(t, deploymentName, result.RoutingConfig.GetCurrentDeploymentVersion().GetDeploymentName())
	require.Equal(t, buildID, result.RoutingConfig.GetCurrentDeploymentVersion().GetBuildId())
}

func TestIsRetryableUpdateError(t *testing.T) {
	t.Run("returns true for errUpdateInProgress", func(t *testing.T) {
		require.True(t, isRetryableUpdateError(errUpdateInProgress))
	})

	t.Run("returns true for errWorkflowHistoryTooLong", func(t *testing.T) {
		require.True(t, isRetryableUpdateError(errWorkflowHistoryTooLong))
	})

	t.Run("returns true for ErrWorkflowClosing", func(t *testing.T) {
		// consts.ErrWorkflowClosing is the actual error that matches by string comparison
		require.True(t, isRetryableUpdateError(consts.ErrWorkflowClosing))
	})

	t.Run("returns true for AbortedByServerErr", func(t *testing.T) {
		// update2.AbortedByServerErr is the actual error that matches by string comparison
		require.True(t, isRetryableUpdateError(update2.AbortedByServerErr))
	})

	t.Run("returns true for ResourceExhausted with CONCURRENT_LIMIT cause", func(t *testing.T) {
		err := &serviceerror.ResourceExhausted{
			Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
		}
		require.True(t, isRetryableUpdateError(err))
	})

	t.Run("returns true for ResourceExhausted with BUSY_WORKFLOW cause", func(t *testing.T) {
		err := &serviceerror.ResourceExhausted{
			Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		}
		require.True(t, isRetryableUpdateError(err))
	})

	t.Run("returns false for ResourceExhausted with other causes", func(t *testing.T) {
		err := &serviceerror.ResourceExhausted{
			Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
		}
		require.False(t, isRetryableUpdateError(err))
	})

	t.Run("returns true for WorkflowNotReady error", func(t *testing.T) {
		err := serviceerror.NewWorkflowNotReady("test message")
		require.True(t, isRetryableUpdateError(err))
	})

	t.Run("handles MultiOperationExecution with nil entries without panic", func(t *testing.T) {
		// This is the critical test case - MultiOperationExecution can have nil entries
		// when one operation succeeds and another fails
		err := serviceerror.NewMultiOperationExecution("multi-op failed", []error{
			nil, // Start operation succeeded
			serviceerror.NewInvalidArgument("update failed"),
		})
		// Should not panic and should return false (InvalidArgument is not retryable)
		require.False(t, isRetryableUpdateError(err))
	})

	t.Run("returns true for MultiOperationExecution with retryable ResourceExhausted", func(t *testing.T) {
		err := serviceerror.NewMultiOperationExecution("multi-op failed", []error{
			serviceerror.NewInvalidArgument("start failed"),
			&serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			},
		})
		require.True(t, isRetryableUpdateError(err))
	})

	t.Run("handles MultiOperationExecution with mix of nil and retryable errors", func(t *testing.T) {
		err := serviceerror.NewMultiOperationExecution("multi-op failed", []error{
			nil, // First operation succeeded
			&serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
			},
		})
		require.True(t, isRetryableUpdateError(err))
	})

	t.Run("returns true for MultiOperationExecution with ErrWorkflowClosing", func(t *testing.T) {
		err := serviceerror.NewMultiOperationExecution("multi-op failed", []error{
			nil, // Start operation succeeded
			consts.ErrWorkflowClosing,
		})
		require.True(t, isRetryableUpdateError(err))
	})

	t.Run("returns true for MultiOperationExecution with AbortedByServerErr", func(t *testing.T) {
		err := serviceerror.NewMultiOperationExecution("multi-op failed", []error{
			serviceerror.NewInvalidArgument("start failed"),
			update2.AbortedByServerErr,
		})
		require.True(t, isRetryableUpdateError(err))
	})

	t.Run("returns false for non-retryable errors", func(t *testing.T) {
		err := serviceerror.NewInvalidArgument("test error")
		require.False(t, isRetryableUpdateError(err))
	})
}

func TestIsRetryableQueryError(t *testing.T) {
	t.Run("returns false for Internal error", func(t *testing.T) {
		// Internal errors are considered retryable by api.IsRetryableError
		// but isRetryableQueryError explicitly excludes them
		err := serviceerror.NewInternal("internal error")
		require.False(t, isRetryableQueryError(err))
	})

	t.Run("returns true for Unavailable error", func(t *testing.T) {
		// Unavailable is retryable and not Internal
		err := serviceerror.NewUnavailable("service unavailable")
		require.True(t, isRetryableQueryError(err))
	})

	t.Run("returns true for consts.ErrStaleState", func(t *testing.T) {
		// This is one of the errors that api.IsRetryableError considers retryable
		require.True(t, isRetryableQueryError(consts.ErrStaleState))
	})

	t.Run("returns true for consts.ErrLocateCurrentWorkflowExecution", func(t *testing.T) {
		require.True(t, isRetryableQueryError(consts.ErrLocateCurrentWorkflowExecution))
	})

	t.Run("returns true for consts.ErrBufferedQueryCleared", func(t *testing.T) {
		require.True(t, isRetryableQueryError(consts.ErrBufferedQueryCleared))
	})

	t.Run("returns false for non-retryable errors", func(t *testing.T) {
		// InvalidArgument is not retryable
		err := serviceerror.NewInvalidArgument("invalid argument")
		require.False(t, isRetryableQueryError(err))
	})

	t.Run("returns false for NotFound error", func(t *testing.T) {
		err := serviceerror.NewNotFound("not found")
		require.False(t, isRetryableQueryError(err))
	})

	t.Run("returns false for AlreadyExists error", func(t *testing.T) {
		err := serviceerror.NewAlreadyExists("already exists")
		require.False(t, isRetryableQueryError(err))
	})

	t.Run("returns false for FailedPrecondition error", func(t *testing.T) {
		err := serviceerror.NewFailedPrecondition("failed precondition")
		require.False(t, isRetryableQueryError(err))
	})

	t.Run("returns false for wrapped Internal error", func(t *testing.T) {
		// Even if wrapped, Internal errors should be excluded
		internalErr := serviceerror.NewInternal("internal error")
		wrappedErr := fmt.Errorf("wrapped: %w", internalErr)
		require.False(t, isRetryableQueryError(wrappedErr))
	})

	t.Run("returns true for MultiOperationExecution with Unavailable error", func(t *testing.T) {
		// MultiOperationExecution with Unavailable (retryable, not Internal)
		err := serviceerror.NewMultiOperationExecution("multi-op", []error{
			serviceerror.NewUnavailable("unavailable"),
			nil,
		})
		require.True(t, isRetryableQueryError(err))
	})

	t.Run("returns true for MultiOperationExecution with Internal error", func(t *testing.T) {
		// api.IsRetryableError returns true for MultiOp with Internal errors
		err := serviceerror.NewMultiOperationExecution("multi-op", []error{
			serviceerror.NewInternal("internal error"),
			nil,
		})
		require.True(t, isRetryableQueryError(err))
	})

	t.Run("returns false for MultiOperationExecution with non-retryable errors", func(t *testing.T) {
		err := serviceerror.NewMultiOperationExecution("multi-op", []error{
			serviceerror.NewInvalidArgument("invalid"),
			serviceerror.NewNotFound("not found"),
		})
		require.False(t, isRetryableQueryError(err))
	})
}
