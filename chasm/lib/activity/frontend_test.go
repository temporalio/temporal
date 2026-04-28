package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/durationpb"
)

type hasRequestID interface {
	GetRequestId() string
}

// TestRequestIdStableAcrossRetries verifies that a request ID is re-used
// across retries, even if server-generated.
func TestRequestIdStableAcrossRetries(t *testing.T) {
	h := &frontendHandler{
		config: &Config{
			BlobSizeLimitError:         defaultBlobSizeLimitError,
			BlobSizeLimitWarn:          defaultBlobSizeLimitWarn,
			MaxIDLengthLimit:           func() int { return defaultMaxIDLengthLimit },
			DefaultActivityRetryPolicy: getDefaultRetrySettings,
		},
		logger: log.NewNoopLogger(),
	}
	nsID := namespace.ID("test-namespace-id")

	newReq := func(requestId string) *workflowservice.StartActivityExecutionRequest {
		return &workflowservice.StartActivityExecutionRequest{
			Namespace:  "test-namespace",
			ActivityId: "test-activity",
			ActivityType: &commonpb.ActivityType{
				Name: "test-type",
			},
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: "test-queue",
			},
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           requestId,
		}
	}

	// Simulate two RetryableInterceptor attempts: both call
	// validateAndPopulateStartRequest with the same request pointer.
	validateTwoAttempts := func(t *testing.T, req *workflowservice.StartActivityExecutionRequest) {
		t.Helper()
		clone1, err := h.validateAndPopulateStartRequest(req, nsID)
		require.NoError(t, err)
		require.NotEmpty(t, clone1.RequestId)

		clone2, err := h.validateAndPopulateStartRequest(req, nsID)
		require.NoError(t, err)
		require.Equal(t, clone1.RequestId, clone2.RequestId)
	}

	// validateTwice calls validate twice and asserts the request ID is stable.
	validateTwice := func(t *testing.T, req hasRequestID, validate func() error) {
		t.Helper()
		require.NoError(t, validate())
		require.NotEmpty(t, req.GetRequestId())
		firstID := req.GetRequestId()
		require.NoError(t, validate())
		require.Equal(t, firstID, req.GetRequestId())
	}

	t.Run("start/server-generated", func(t *testing.T) {
		validateTwoAttempts(t, newReq(""))
	})

	t.Run("start/client-provided", func(t *testing.T) {
		validateTwoAttempts(t, newReq("my-request-id"))
	})

	t.Run("terminate/server-generated", func(t *testing.T) {
		req := &workflowservice.TerminateActivityExecutionRequest{
			Namespace:  "test-namespace",
			ActivityId: "test-activity",
		}
		validateTwice(t, req, func() error {
			return validateTerminateActivityExecutionRequest(
				req, defaultMaxIDLengthLimit, defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, log.NewNoopLogger())
		})
	})

	t.Run("cancel/server-generated", func(t *testing.T) {
		req := &workflowservice.RequestCancelActivityExecutionRequest{
			Namespace:  "test-namespace",
			ActivityId: "test-activity",
		}
		validateTwice(t, req, func() error {
			return validateRequestCancelActivityExecutionRequest(
				req, defaultMaxIDLengthLimit, defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, log.NewNoopLogger())
		})
	})
}
