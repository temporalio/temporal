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
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
)

type (
	// NamespaceValidatorInterceptor contains LengthValidationIntercept and StateValidationIntercept
	NamespaceValidatorInterceptor struct {
		namespaceRegistry               namespace.Registry
		tokenSerializer                 common.TaskTokenSerializer
		enableTokenNamespaceEnforcement dynamicconfig.BoolPropertyFn
		maxNamespaceLength              dynamicconfig.IntPropertyFn
	}
)

var (
	ErrNamespaceNotSet            = serviceerror.NewInvalidArgument("Namespace not set on request.")
	errNamespaceTooLong           = serviceerror.NewInvalidArgument("Namespace length exceeds limit.")
	errNamespaceHandover          = serviceerror.NewUnavailable(fmt.Sprintf("Namespace replication in %s state.", enumspb.REPLICATION_STATE_HANDOVER.String()))
	errTaskTokenNotSet            = serviceerror.NewInvalidArgument("Task token not set on request.")
	errTaskTokenNamespaceMismatch = serviceerror.NewInvalidArgument("Operation requested with a token from a different namespace.")

	allowedNamespaceStates = map[string][]enumspb.NamespaceState{
		"StartWorkflowExecution":           {enumspb.NAMESPACE_STATE_REGISTERED},
		"SignalWithStartWorkflowExecution": {enumspb.NAMESPACE_STATE_REGISTERED},
	}
	// If API name is not in the map above, these are allowed states for all APIs that have `namespace` or `task_token` field in the request object.
	defaultAllowedNamespaceStates = []enumspb.NamespaceState{enumspb.NAMESPACE_STATE_REGISTERED, enumspb.NAMESPACE_STATE_DEPRECATED}

	allowedMethodsDuringHandover = map[string]struct{}{
		"UpdateNamespace":                  {},
		"GetReplicationMessages":           {},
		"ReplicateEventsV2":                {},
		"GetWorkflowExecutionRawHistoryV2": {},
	}
)

var _ grpc.UnaryServerInterceptor = (*NamespaceValidatorInterceptor)(nil).StateValidationIntercept
var _ grpc.UnaryServerInterceptor = (*NamespaceValidatorInterceptor)(nil).LengthValidationIntercept

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

func (ni *NamespaceValidatorInterceptor) LengthValidationIntercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	reqWithNamespace, hasNamespace := req.(NamespaceNameGetter)
	if hasNamespace {
		namespaceName := namespace.Name(reqWithNamespace.GetNamespace())
		if len(namespaceName) > ni.maxNamespaceLength() {
			return nil, errNamespaceTooLong
		}
	}

	return handler(ctx, req)
}

// StateValidationIntercept validates:
// 1. Namespace is specified in task token if there is a `task_token` field.
// 2. Namespace is specified in request if there is a `namespace` field and no `task_token` field.
// 3. Namespace exists.
// 4. Namespace from request match namespace from task token, if check is enabled with dynamic config.
// 5. Namespace is in correct state.
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

	err = ni.checkNamespaceState(namespaceEntry, info.FullMethod)
	if err != nil {
		return nil, err
	}
	err = ni.checkReplicationState(namespaceEntry, info.FullMethod)
	if err != nil {
		return nil, err
	}

	return handler(ctx, req)
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
			return nil, ErrNamespaceNotSet
		}
		return nil, nil
	case *workflowservice.RegisterNamespaceRequest:
		// Special case for RegisterNamespace API. `namespaceName` is name of namespace that about to be registered.
		// There is no namespace entry for it, therefore, it must bypass namespace registry and validator.
		if namespaceName.IsEmpty() {
			return nil, ErrNamespaceNotSet
		}
		return nil, nil
	default:
		// All other APIs.
		if namespaceName.IsEmpty() {
			return nil, ErrNamespaceNotSet
		}
		return ni.namespaceRegistry.GetNamespace(namespaceName)
	}
}

func (ni *NamespaceValidatorInterceptor) extractNamespaceFromTaskToken(req interface{}) (*namespace.Namespace, error) {
	reqWithTaskToken, hasTaskToken := req.(interface{ GetTaskToken() []byte })
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
		return nil, ErrNamespaceNotSet
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

	_, methodName := splitMethodName(fullMethod)

	allowedStates, allowedStatesDefined := allowedNamespaceStates[methodName]
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

	_, methodName := splitMethodName(fullMethod)

	if _, ok := allowedMethodsDuringHandover[methodName]; ok {
		return nil
	}

	return errNamespaceHandover
}
