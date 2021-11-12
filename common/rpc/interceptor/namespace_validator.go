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
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/namespace"
)

type (
	// NamespaceValidatorInterceptor validates:
	// 1. Namespace is specified in request if there is a `namespace` field.
	// 2. Namespace exists.
	// 3. Namespace is in correct state.
	NamespaceValidatorInterceptor struct {
		namespaceRegistry namespace.Registry
	}
)

var (
	ErrNamespaceNotSet              = serviceerror.NewInvalidArgument("Namespace not set on request.")
	errInvalidNamespaceStateMessage = "Namespace has invalid state: %s. Must be %s."

	// By default, enumspb.NAMESPACE_STATE_REGISTERED and enumspb.NAMESPACE_STATE_DEPRECATED are allowed states for all APIs that have `namespace` field in the request object.
	allowedNamespaceStates = map[string][]enumspb.NamespaceState{
		"StartWorkflowExecution":           {enumspb.NAMESPACE_STATE_REGISTERED},
		"SignalWithStartWorkflowExecution": {enumspb.NAMESPACE_STATE_REGISTERED},
		"DescribeNamespace":                {enumspb.NAMESPACE_STATE_REGISTERED, enumspb.NAMESPACE_STATE_DEPRECATED, enumspb.NAMESPACE_STATE_DELETED},
	}
)

var _ grpc.UnaryServerInterceptor = (*NamespaceValidatorInterceptor)(nil).Intercept

func NewNamespaceValidatorInterceptor(
	namespaceRegistry namespace.Registry,
) *NamespaceValidatorInterceptor {
	return &NamespaceValidatorInterceptor{
		namespaceRegistry: namespaceRegistry,
	}
}

func (ni *NamespaceValidatorInterceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	namespaceEntry, err := ni.extractNamespace(req)
	if err != nil {
		return nil, err
	}
	if namespaceEntry != nil {
		_, methodName := splitMethodName(info.FullMethod)
		allowedStates, allowedStatesDefined := allowedNamespaceStates[methodName]
		if !allowedStatesDefined {
			// If not explicitly defined, only enumspb.NAMESPACE_STATE_REGISTERED and enumspb.NAMESPACE_STATE_DEPRECATED are allowed.
			if namespaceEntry.State() != enumspb.NAMESPACE_STATE_REGISTERED && namespaceEntry.State() != enumspb.NAMESPACE_STATE_DEPRECATED {
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errInvalidNamespaceStateMessage, namespaceEntry.State(), fmt.Sprintf("%s or %s", enumspb.NAMESPACE_STATE_REGISTERED.String(), enumspb.NAMESPACE_STATE_DEPRECATED.String())))
			}
		} else {
			isStateAllowed := false
			for _, allowedState := range allowedStates {
				if allowedState == namespaceEntry.State() {
					isStateAllowed = true
					break
				}
			}
			if !isStateAllowed {
				var allowedStatesStr []string
				for _, allowedState := range allowedStates {
					allowedStatesStr = append(allowedStatesStr, allowedState.String())
				}
				return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(errInvalidNamespaceStateMessage, namespaceEntry.State(), strings.Join(allowedStatesStr, " or ")))
			}
		}
	}

	return handler(ctx, req)
}

func (ni *NamespaceValidatorInterceptor) extractNamespace(req interface{}) (*namespace.Namespace, error) {
	if reqWithNamespace, ok := req.(NamespaceNameGetter); ok {
		namespaceName := namespace.Name(reqWithNamespace.GetNamespace())

		if !namespaceName.IsEmpty() {
			// Special case for "RegisterNamespace" API. `namespaceName` is name of namespace that about to be registered. There is no namespace entry for it.
			if _, isRegisterNamespace := req.(*workflowservice.RegisterNamespaceRequest); isRegisterNamespace {
				return nil, nil
			}

			namespaceEntry, err := ni.namespaceRegistry.GetNamespace(namespaceName)
			if err != nil {
				return nil, err
			}
			return namespaceEntry, nil
		}

		// Special case for "DescribeNamespace" API which can get namespace by Id.
		dnr, isDescribeNamespace := req.(*workflowservice.DescribeNamespaceRequest)
		if !isDescribeNamespace || dnr.GetId() == "" {
			return nil, ErrNamespaceNotSet
		}

		namespaceEntry, err := ni.namespaceRegistry.GetNamespaceByID(namespace.ID(dnr.GetId()))
		if err != nil {
			return nil, err
		}
		return namespaceEntry, nil
	}

	return nil, nil
}
