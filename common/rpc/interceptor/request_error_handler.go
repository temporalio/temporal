package interceptor

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination request_error_handler_mock.go

import (
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/grpc/codes"
)

type (
	// ErrorHandler defines the interface for handling request errors
	ErrorHandler interface {
		HandleError(
			req any,
			fullMethod string,
			metricsHandler metrics.Handler,
			logTags []tag.Tag,
			err error,
			nsName namespace.Name,
		)
	}

	// RequestErrorHandler handles error recording and logging for RPC interceptors
	RequestErrorHandler struct {
		logger          log.Logger
		workflowTags    *logtags.WorkflowTags
		logAllReqErrors dynamicconfig.BoolPropertyFnWithNamespaceFilter
	}
)

// NewRequestErrorHandler creates a new RequestErrorHandler
func NewRequestErrorHandler(
	logger log.Logger,
	logAllReqErrors dynamicconfig.BoolPropertyFnWithNamespaceFilter,
) *RequestErrorHandler {
	return &RequestErrorHandler{
		logger:          logger,
		workflowTags:    logtags.NewWorkflowTags(tasktoken.NewSerializer(), logger),
		logAllReqErrors: logAllReqErrors,
	}
}

// HandleError handles error recording and logging
func (eh *RequestErrorHandler) HandleError(
	req any,
	fullMethod string,
	metricsHandler metrics.Handler,
	logTags []tag.Tag,
	err error,
	nsName namespace.Name,
) {
	statusCode := serviceerror.ToStatus(err).Code()
	if statusCode == codes.OK {
		return
	}

	isExpectedError := isExpectedErrorByStatusCode(statusCode) || isExpectedErrorByType(err)

	recordErrorMetrics(metricsHandler, err, isExpectedError)
	eh.logError(req, fullMethod, nsName, err, statusCode, isExpectedError, logTags)
}

func (eh *RequestErrorHandler) logError(
	req any,
	fullMethod string,
	nsName namespace.Name,
	err error,
	statusCode codes.Code,
	isExpectedError bool,
	logTags []tag.Tag,
) {
	logAllErrors := nsName != "" && eh.logAllReqErrors(nsName.String())
	// context errors may not be user errors, but still too noisy to log by default
	if !logAllErrors && (isExpectedError ||
		common.IsContextDeadlineExceededErr(err) ||
		common.IsContextCanceledErr(err) ||
		common.IsResourceExhausted(err)) {
		return
	}

	logTags = append(logTags, tag.NewStringerTag("grpc_code", statusCode))
	logTags = append(logTags, eh.workflowTags.Extract(req, fullMethod)...)

	eh.logger.Error("service failures", append(logTags, tag.Error(err))...)
}

func recordErrorMetrics(metricsHandler metrics.Handler, err error, isExpectedError bool) {
	metrics.ServiceErrorWithType.With(metricsHandler).Record(1, metrics.ServiceErrorTypeTag(err))

	var resourceExhaustedErr *serviceerror.ResourceExhausted
	if errors.As(err, &resourceExhaustedErr) {
		metrics.ServiceErrResourceExhaustedCounter.With(metricsHandler).Record(
			1,
			metrics.ResourceExhaustedCauseTag(resourceExhaustedErr.Cause),
			metrics.ResourceExhaustedScopeTag(resourceExhaustedErr.Scope),
		)
	}

	if isExpectedError {
		return
	}

	metrics.ServiceFailures.With(metricsHandler).Record(1)
}

//nolint:revive // explicit cases in switch for documentation purposes
func isExpectedErrorByStatusCode(statusCode codes.Code) bool {
	switch statusCode {
	case codes.Canceled,
		codes.InvalidArgument,
		codes.NotFound,
		codes.AlreadyExists,
		codes.PermissionDenied,
		codes.FailedPrecondition,
		codes.OutOfRange,
		codes.Unauthenticated:
		return true
	// We could just return false here, but making it explicit what codes are
	// considered (potentially) server errors.
	case codes.Unknown,
		codes.DeadlineExceeded,
		// the result for resource exhausted depends on the resource exhausted scope and
		// will be handled by isExpectedErrorByType()
		codes.ResourceExhausted,
		codes.Aborted,
		codes.Unimplemented,
		codes.Internal,
		codes.Unavailable,
		codes.DataLoss:
		return false
	default:
		return false
	}
}

func isExpectedErrorByType(err error) bool {
	// This is not a full list of service errors.
	// Only errors with status code that fails the isExpectedErrorByStatusCode() check
	// but are actually expected need to be explicitly handled here.
	//
	// Some of the errors listed below does not failed the isExpectedErrorByStatusCode() check
	// but are listed nonetheless.
	switch err := err.(type) {
	case *serviceerror.ResourceExhausted:
		return err.Scope == enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE
	case *serviceerror.Canceled,
		*serviceerror.AlreadyExists,
		*serviceerror.CancellationAlreadyRequested,
		*serviceerror.FailedPrecondition,
		*serviceerror.NamespaceInvalidState,
		*serviceerror.NamespaceNotActive,
		*serviceerror.NamespaceNotFound,
		*serviceerror.NamespaceAlreadyExists,
		*serviceerror.InvalidArgument,
		*serviceerror.WorkflowExecutionAlreadyStarted,
		*serviceerror.WorkflowNotReady,
		*serviceerror.NotFound,
		*serviceerror.QueryFailed,
		*serviceerror.ClientVersionNotSupported,
		*serviceerror.ServerVersionNotSupported,
		*serviceerror.PermissionDenied,
		*serviceerror.NewerBuildExists,
		*serviceerrors.StickyWorkerUnavailable,
		*serviceerrors.TaskAlreadyStarted,
		*serviceerrors.RetryReplication,
		*serviceerrors.SyncState:
		return true
	default:
		return false
	}
}
