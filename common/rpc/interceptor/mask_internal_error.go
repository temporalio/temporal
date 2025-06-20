package interceptor

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	"go.temporal.io/server/common/tasktoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errorFrontendMasked = "something went wrong, please retry"

type MaskInternalErrorDetailsInterceptor struct {
	maskInternalError dynamicconfig.BoolPropertyFnWithNamespaceFilter
	namespaceRegistry namespace.Registry
	logger            log.Logger
	workflowTags      *logtags.WorkflowTags
}

func NewMaskInternalErrorDetailsInterceptor(
	maskErrorSetting dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
) *MaskInternalErrorDetailsInterceptor {

	return &MaskInternalErrorDetailsInterceptor{
		maskInternalError: maskErrorSetting,
		namespaceRegistry: namespaceRegistry,
		workflowTags:      logtags.NewWorkflowTags(tasktoken.NewSerializer(), logger),
		logger:            logger,
	}
}

func (mi *MaskInternalErrorDetailsInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	resp, err := handler(ctx, req)

	if err != nil && mi.shouldMaskErrors(req) {
		err = mi.maskUnknownOrInternalErrors(req, info.FullMethod, err)
	}
	return resp, err
}

func (mi *MaskInternalErrorDetailsInterceptor) shouldMaskErrors(req any) bool {
	ns := MustGetNamespaceName(mi.namespaceRegistry, req)
	if ns.IsEmpty() {
		return false
	}
	return mi.maskInternalError(ns.String())
}

func (mi *MaskInternalErrorDetailsInterceptor) maskUnknownOrInternalErrors(
	req interface{}, fullMethodName string, err error,
) error {
	statusCode := serviceerror.ToStatus(err).Code()

	if statusCode != codes.Unknown && statusCode != codes.Internal {
		return err
	}

	// we need to log the original error with hash.
	// This code is similar to the one in telemetry.go

	// convert internal and unknown errors into neutral error with hash code of the original error
	errorHash := common.ErrorHash(err)
	// logging the error with hash code
	mi.logError(req, fullMethodName, err, errorHash, statusCode)

	// returning masked error
	maskedErrorMessage := fmt.Sprintf("%s (%s)", errorFrontendMasked, errorHash)
	return status.New(statusCode, maskedErrorMessage).Err()
}

func (mi *MaskInternalErrorDetailsInterceptor) logError(
	req any,
	fullMethod string,
	err error,
	errorHash string,
	statusCode codes.Code,
) {
	methodName := api.MethodName(fullMethod)
	overridedMethodName := telemetryOverrideOperationTag(fullMethod, methodName)
	nsName := MustGetNamespaceName(mi.namespaceRegistry, req)
	var logTags []tag.Tag
	if nsName == "" {
		logTags = []tag.Tag{tag.Operation(overridedMethodName)}
	} else {
		logTags = []tag.Tag{tag.Operation(overridedMethodName), tag.WorkflowNamespace(nsName.String())}
	}

	logTags = append(logTags, tag.NewStringTag("hash", errorHash))

	logTags = append(logTags, tag.NewStringerTag("grpc_code", statusCode))
	logTags = append(logTags, mi.workflowTags.Extract(req, fullMethod)...)

	mi.logger.Error("masked service failures", append(logTags, tag.Error(err))...)
}
