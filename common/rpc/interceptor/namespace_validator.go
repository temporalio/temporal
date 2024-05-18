// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package interceptor

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
)

type (
	TaskTokenGetter interface {
		GetTaskToken() []byte
	}

	// NamespaceValidatorInterceptor contains NamespaceValidateIntercept and StateValidationIntercept
	NamespaceValidatorInterceptor struct {
		namespaceRegistry               namespace.Registry
		tokenSerializer                 common.TaskTokenSerializer
		enableTokenNamespaceEnforcement dynamicconfig.BoolPropertyFn
		maxNamespaceLength              dynamicconfig.IntPropertyFn
	}
)

var (
	errNamespaceNotSet            = serviceerror.NewInvalidArgument("Namespace not set on request.")
	errBothNamespaceIDAndNameSet  = serviceerror.NewInvalidArgument("Only one of namespace name or Id should be set on request.")
	errNamespaceTooLong           = serviceerror.NewInvalidArgument("Namespace length exceeds limit.")
	errTaskTokenNotSet            = serviceerror.NewInvalidArgument("Task token not set on request.")
	errTaskTokenNamespaceMismatch = serviceerror.NewInvalidArgument("Operation requested with a token from a different namespace.")

	allowedNamespaceStates = map[string][]enumspb.NamespaceState{
		"/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution":           {enumspb.NAMESPACE_STATE_REGISTERED},
		"/temporal.api.workflowservice.v1.WorkflowService/SignalWithStartWorkflowExecution": {enumspb.NAMESPACE_STATE_REGISTERED},
		"/temporal.api.operatorservice.v1.OperatorService/DeleteNamespace":                  {enumspb.NAMESPACE_STATE_REGISTERED, enumspb.NAMESPACE_STATE_DEPRECATED, enumspb.NAMESPACE_STATE_DELETED},
		"/temporal.api.nexusservice.v1.NexusService/DispatchNexusTask":                      {enumspb.NAMESPACE_STATE_REGISTERED},
	}
	// If API name is not in the map above, these are allowed states for all APIs that have `namespace` or `task_token` field in the request object.
	defaultAllowedNamespaceStates = []enumspb.NamespaceState{enumspb.NAMESPACE_STATE_REGISTERED, enumspb.NAMESPACE_STATE_DEPRECATED}

	allowedMethodsDuringHandover = map[string]struct{}{
		"DescribeNamespace":                  {},
		"UpdateNamespace":                    {},
		"GetReplicationMessages":             {},
		"ReplicateEventsV2":                  {},
		"GetWorkflowExecutionRawHistory":     {},
		"GetWorkflowExecutionRawHistoryV2":   {},
		"GetWorkflowExecutionHistory":        {},
		"GetWorkflowExecutionHistoryReverse": {},
		"DescribeWorkflowExecution":          {},
		"DescribeTaskQueue":                  {},
		"ListTaskQueuePartitions":            {},
		"ListOpenWorkflowExecutions":         {},
		"ListClosedWorkflowExecutions":       {},
		"ListWorkflowExecutions":             {},
		"ListArchivedWorkflowExecutions":     {},
		"ScanWorkflowExecutions":             {},
		"CountWorkflowExecutions":            {},
		"DescribeSchedule":                   {},
		"ListScheduleMatchingTimes":          {},
		"ListSchedules":                      {},
		"GetWorkerBuildIdCompatibility":      {},
		"GetWorkerVersioningRules":           {},
		"GetWorkerTaskReachability":          {},
		"DescribeBatchOperation":             {},
		"ListBatchOperations":                {},
	}
)

var _ grpc.UnaryServerInterceptor = (*NamespaceValidatorInterceptor)(nil).StateValidationIntercept
var _ grpc.UnaryServerInterceptor = (*NamespaceValidatorInterceptor)(nil).NamespaceValidateIntercept

func NewNamespaceValidatorInterceptor(
	namespaceRegistry namespace.Registry,
	enableTokenNamespaceEnforcement dynamicconfig.BoolPropertyFn,
	maxNamespaceLength dynamicconfig.IntPropertyFn,
) *NamespaceValidatorInterceptor {
	return &NamespaceValidatorInterceptor{
		namespaceRegistry:               namespaceRegistry,
		tokenSerializer:                 common.NewProtoTaskTokenSerializer(),
		enableTokenNamespaceEnforcement: enableTokenNamespaceEnforcement,
		maxNamespaceLength:              maxNamespaceLength,
	}
}

func (ni *NamespaceValidatorInterceptor) NamespaceValidateIntercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	err := ni.setNamespaceIfNotPresent(req)
	if err != nil {
		return nil, err
	}
	reqWithNamespace, hasNamespace := req.(NamespaceNameGetter)
	if hasNamespace {
		if err := ni.ValidateName(reqWithNamespace.GetNamespace()); err != nil {
			return nil, err
		}
	}

	return handler(ctx, req)
}

// ValidateName validates a namespace name (currently only a max length check).
func (ni *NamespaceValidatorInterceptor) ValidateName(ns string) error {
	if len(ns) > ni.maxNamespaceLength() {
		return errNamespaceTooLong
	}
	return nil
}

func (ni *NamespaceValidatorInterceptor) setNamespaceIfNotPresent(
	req interface{},
) error {
	switch request := req.(type) {
	case NamespaceNameGetter:
		if request.GetNamespace() == "" {
			namespaceEntry, err := ni.extractNamespaceFromTaskToken(req)
			if err != nil {
				return err
			}
			ni.setNamespace(namespaceEntry, req)
		}
		return nil
	default:
		return nil
	}
}

func (ni *NamespaceValidatorInterceptor) setNamespace(
	namespaceEntry *namespace.Namespace,
	req interface{},
) {
	switch request := req.(type) {
	case *workflowservice.RespondQueryTaskCompletedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondWorkflowTaskCompletedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondWorkflowTaskFailedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RecordActivityTaskHeartbeatRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondActivityTaskCanceledRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondActivityTaskCompletedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondActivityTaskFailedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondNexusTaskCompletedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	case *workflowservice.RespondNexusTaskFailedRequest:
		if request.Namespace == "" {
			request.Namespace = namespaceEntry.Name().String()
		}
	}
}

// StateValidationIntercept runs ValidateState - see docstring for that method.
func (ni *NamespaceValidatorInterceptor) StateValidationIntercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	namespaceEntry, err := ni.extractNamespace(req)
	if err != nil {
		return nil, err
	}

	if err := ni.ValidateState(namespaceEntry, info.FullMethod); err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

// ValidateState validates:
// 1. Namespace is specified in task token if there is a `task_token` field.
// 2. Namespace is specified in request if there is a `namespace` field and no `task_token` field.
// 3. Namespace exists.
// 4. Namespace from request match namespace from task token, if check is enabled with dynamic config.
// 5. Namespace is in correct state.
func (ni *NamespaceValidatorInterceptor) ValidateState(namespaceEntry *namespace.Namespace, fullMethod string) error {
	if err := ni.checkNamespaceState(namespaceEntry, fullMethod); err != nil {
		return err
	}
	return ni.checkReplicationState(namespaceEntry, fullMethod)
}

func (ni *NamespaceValidatorInterceptor) extractNamespace(req interface{}) (*namespace.Namespace, error) {
	// Token namespace has priority over request namespace. Check it first.
	tokenNamespaceEntry, tokenErr := ni.extractNamespaceFromTaskToken(req)
	if tokenErr != nil {
		return nil, tokenErr
	}

	requestNamespaceEntry, requestErr := ni.extractNamespaceFromRequest(req)
	// If namespace was extracted from token then it will be used.
	if requestErr != nil && tokenNamespaceEntry == nil {
		return nil, requestErr
	}

	err := ni.checkNamespaceMatch(requestNamespaceEntry, tokenNamespaceEntry)
	if err != nil {
		return nil, err
	}

	// Use namespace from task token (if specified) and ignore namespace from request.
	if tokenNamespaceEntry != nil {
		return tokenNamespaceEntry, nil
	}

	return requestNamespaceEntry, nil
}

func (ni *NamespaceValidatorInterceptor) extractNamespaceFromRequest(req interface{}) (*namespace.Namespace, error) {
	reqWithNamespace, hasNamespace := req.(NamespaceNameGetter)
	if !hasNamespace {
		return nil, nil
	}
	namespaceName := namespace.Name(reqWithNamespace.GetNamespace())

	switch request := req.(type) {
	case *workflowservice.DescribeNamespaceRequest:
		// Special case for DescribeNamespace API which should read namespace directly from database.
		// Therefore, it must bypass namespace registry and validator.
		if request.GetId() == "" && namespaceName.IsEmpty() {
			return nil, errNamespaceNotSet
		}
		return nil, nil
	case *adminservice.GetNamespaceRequest:
		// special case for Admin.GetNamespace API which accept either Namespace ID or Namespace name as input
		if request.GetId() == "" && namespaceName.IsEmpty() {
			return nil, errNamespaceNotSet
		}
		return nil, nil
	case *workflowservice.RegisterNamespaceRequest:
		// Special case for RegisterNamespace API. `namespaceName` is name of namespace that about to be registered.
		// There is no namespace entry for it, therefore, it must bypass namespace registry and validator.
		if namespaceName.IsEmpty() {
			return nil, errNamespaceNotSet
		}
		return nil, nil
	case *operatorservice.DeleteNamespaceRequest:
		// special case for Operator.DeleteNamespace API which accept either Namespace ID or Namespace name as input
		namespaceID := namespace.ID(request.GetNamespaceId())
		if namespaceID.IsEmpty() && namespaceName.IsEmpty() {
			return nil, errNamespaceNotSet
		}
		if !namespaceID.IsEmpty() && !namespaceName.IsEmpty() {
			return nil, errBothNamespaceIDAndNameSet
		}
		if namespaceID != "" {
			return ni.namespaceRegistry.GetNamespaceByID(namespaceID)
		}
		return ni.namespaceRegistry.GetNamespace(namespaceName)
	case *adminservice.DescribeHistoryHostRequest:
		// Special case for DescribeHistoryHost API which should run regardless of namespace state.
		return nil, nil
	case *adminservice.AddSearchAttributesRequest,
		*adminservice.RemoveSearchAttributesRequest,
		*adminservice.GetSearchAttributesRequest,
		*operatorservice.AddSearchAttributesRequest,
		*operatorservice.RemoveSearchAttributesRequest,
		*operatorservice.ListSearchAttributesRequest:
		// Namespace is optional for search attributes operations.
		// It's required when using SQL DB for visibility, but not when using Elasticsearch.
		if !namespaceName.IsEmpty() {
			return ni.namespaceRegistry.GetNamespace(namespaceName)
		}
		return nil, nil
	default:
		// All other APIs.
		if namespaceName.IsEmpty() {
			return nil, errNamespaceNotSet
		}
		return ni.namespaceRegistry.GetNamespace(namespaceName)
	}
}

func (ni *NamespaceValidatorInterceptor) extractNamespaceFromTaskToken(req interface{}) (*namespace.Namespace, error) {
	reqWithTaskToken, hasTaskToken := req.(TaskTokenGetter)
	if !hasTaskToken {
		return nil, nil
	}
	taskTokenBytes := reqWithTaskToken.GetTaskToken()
	if len(taskTokenBytes) == 0 {
		return nil, errTaskTokenNotSet
	}
	var namespaceID namespace.ID
	// Special case for deprecated RespondQueryTaskCompleted API.
	if _, ok := req.(*workflowservice.RespondQueryTaskCompletedRequest); ok {
		taskToken, err := ni.tokenSerializer.DeserializeQueryTaskToken(taskTokenBytes)
		if err != nil {
			return nil, err
		}
		namespaceID = namespace.ID(taskToken.GetNamespaceId())
	} else {
		taskToken, err := ni.tokenSerializer.Deserialize(taskTokenBytes)
		if err != nil {
			return nil, err
		}
		namespaceID = namespace.ID(taskToken.GetNamespaceId())
	}

	if namespaceID.IsEmpty() {
		return nil, errNamespaceNotSet
	}
	return ni.namespaceRegistry.GetNamespaceByID(namespaceID)
}

func (ni *NamespaceValidatorInterceptor) checkNamespaceMatch(requestNamespace *namespace.Namespace, tokenNamespace *namespace.Namespace) error {
	if tokenNamespace == nil || requestNamespace == nil || !ni.enableTokenNamespaceEnforcement() {
		return nil
	}

	if requestNamespace.ID() != tokenNamespace.ID() {
		return errTaskTokenNamespaceMismatch
	}
	return nil
}

func (ni *NamespaceValidatorInterceptor) checkNamespaceState(namespaceEntry *namespace.Namespace, fullMethod string) error {
	if namespaceEntry == nil {
		return nil
	}

	allowedStates, allowedStatesDefined := allowedNamespaceStates[fullMethod]
	if !allowedStatesDefined {
		allowedStates = defaultAllowedNamespaceStates
	}

	for _, allowedState := range allowedStates {
		if allowedState == namespaceEntry.State() {
			return nil
		}
	}

	return serviceerror.NewNamespaceInvalidState(namespaceEntry.Name().String(), namespaceEntry.State(), allowedStates)
}

func (ni *NamespaceValidatorInterceptor) checkReplicationState(namespaceEntry *namespace.Namespace, fullMethod string) error {
	if namespaceEntry == nil {
		return nil
	}
	if namespaceEntry.ReplicationState() != enumspb.REPLICATION_STATE_HANDOVER {
		return nil
	}

	methodName := api.MethodName(fullMethod)

	if _, ok := allowedMethodsDuringHandover[methodName]; ok {
		return nil
	}

	return common.ErrNamespaceHandover
}
