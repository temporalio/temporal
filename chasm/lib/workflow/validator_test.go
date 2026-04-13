package workflow

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/types/known/durationpb"
)

const testMaxIDLen = 1000

func newTestValidator() *Validator {
	saValidator := searchattribute.NewValidator(
		searchattribute.NewTestProvider(),
		searchattribute.NewTestMapperProvider(nil),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(100),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(1024),
		dynamicconfig.GetIntPropertyFnFilteredByNamespace(4096),
		nil, // visibility manager not needed when SA is nil
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
	)
	return NewValidator(
		Config{
			maxIDLengthLimit: func() int { return testMaxIDLen },
			defaultWorkflowRetrySettings: func(ns string) retrypolicy.DefaultRetrySettings {
				return retrypolicy.DefaultDefaultRetrySettings
			},
			maxLinksPerRequest: func(ns string) int { return 10 },
			linkMaxSize:        func(ns string) int { return 1024 },
		},
		searchattribute.NewTestMapperProvider(nil),
		saValidator,
	)
}

func validSWSRequest() *workflowservice.SignalWithStartWorkflowExecutionRequest {
	return &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:   "test-workflow-id",
		SignalName:   "test-signal",
		WorkflowType: &commonpb.WorkflowType{Name: "test-workflow-type"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "test-task-queue"},
	}
}

func TestValidateSignalWithStartRequest(t *testing.T) {
	v := newTestValidator()

	t.Run("HappyPath", func(t *testing.T) {
		req := validSWSRequest()
		err := v.ValidateSignalWithStartRequest(req)
		require.NoError(t, err)
	})

	t.Run("NilRequest", func(t *testing.T) {
		err := v.ValidateSignalWithStartRequest(nil)
		require.ErrorContains(t, err, "request is empty")
	})

	t.Run("EmptyWorkflowID", func(t *testing.T) {
		req := validSWSRequest()
		req.WorkflowId = ""
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorIs(t, err, ErrWorkflowIDNotSet)
	})

	t.Run("WorkflowIDTooLong", func(t *testing.T) {
		req := validSWSRequest()
		req.WorkflowId = strings.Repeat("a", testMaxIDLen+1)
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorContains(t, err, "WorkflowId exceeds maximum allowed length")
	})

	t.Run("EmptySignalName", func(t *testing.T) {
		req := validSWSRequest()
		req.SignalName = ""
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorContains(t, err, "signal not set")
	})

	t.Run("SignalNameTooLong", func(t *testing.T) {
		req := validSWSRequest()
		req.SignalName = strings.Repeat("s", testMaxIDLen+1)
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorContains(t, err, "signal name exceeds maximum allowed length")
	})

	t.Run("EmptyWorkflowType", func(t *testing.T) {
		req := validSWSRequest()
		req.WorkflowType = &commonpb.WorkflowType{Name: ""}
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorContains(t, err, "workflow type not set")
	})

	t.Run("WorkflowTypeTooLong", func(t *testing.T) {
		req := validSWSRequest()
		req.WorkflowType = &commonpb.WorkflowType{Name: strings.Repeat("t", testMaxIDLen+1)}
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorContains(t, err, "workflow type name exceeds maximum allowed length")
	})

	t.Run("AutoGeneratesRequestID", func(t *testing.T) {
		req := validSWSRequest()
		req.RequestId = ""
		err := v.ValidateSignalWithStartRequest(req)
		require.NoError(t, err)
		require.NotEmpty(t, req.RequestId, "empty RequestId should be auto-populated")
	})

	t.Run("RequestIDTooLong", func(t *testing.T) {
		req := validSWSRequest()
		req.RequestId = strings.Repeat("r", testMaxIDLen+1)
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorContains(t, err, "Request ID exceeds maximum allowed length")
	})

	t.Run("ConflictPolicyFailNotSupported", func(t *testing.T) {
		req := validSWSRequest()
		req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorContains(t, err, "WORKFLOW_ID_CONFLICT_POLICY_FAIL is not supported")
	})

	t.Run("IncompatibleTerminateIfRunningWithConflictPolicy", func(t *testing.T) {
		req := validSWSRequest()
		req.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
		req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorContains(t, err, "WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING cannot be used together with a WorkflowIDConflictPolicy")
	})

	t.Run("IncompatibleRejectDuplicateWithTerminateExisting", func(t *testing.T) {
		req := validSWSRequest()
		req.WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		req.WorkflowIdConflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorContains(t, err, "WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE cannot be used together with WorkflowIdConflictPolicy WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING")
	})

	t.Run("CronAndStartDelaySetTogether", func(t *testing.T) {
		req := validSWSRequest()
		req.CronSchedule = "0 * * * *"
		req.WorkflowStartDelay = durationpb.New(5 * time.Second)
		err := v.ValidateSignalWithStartRequest(req)
		require.ErrorIs(t, err, ErrCronAndStartDelaySet)
	})
}
