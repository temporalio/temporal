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

package authorization

import (
	"context"
	"strings"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/metrics"
)

var (
	errUnauthorized = serviceerror.NewPermissionDenied("Request unauthorized.")
)

var namespaceAPIs = map[string]bool{
	"RegisterNamespace":  true,
	"DescribeNamespace":  true,
	"UpdateNamespace":    true,
	"DeprecateNamespace": true,
}

func (a *interceptor) Interceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	var namespace string
	requestWithNamespace, ok := req.(RequestWithNamespace)
	if ok {
		namespace = requestWithNamespace.GetNamespace()
	}

	apiName := info.FullMethod
	index := strings.LastIndex(apiName, "/")
	if index > -1 {
		apiName = apiName[index+1:]
	}

	if namespace == "" && isNamespaceOp(apiName) {
		namespaceOp, ok := req.(RequestWithName)
		if ok {
			namespace = namespaceOp.GetName()
		}
	}

	scope := a.getMetricsScope(metrics.NumAuthorizationScopes, namespace)
	sw := scope.StartTimer(metrics.ServiceAuthorizationLatency)
	defer sw.Stop()

	result, err := a.authorizer.Authorize(ctx, &Attributes{Namespace: namespace, APIName: apiName})
	if err != nil {
		scope.IncCounter(metrics.ServiceErrAuthorizeFailedCounter)
		return nil, err
	}
	if result.Decision != DecisionAllow {
		scope.IncCounter(metrics.ServiceErrUnauthorizedCounter)
		return nil, errUnauthorized
	}
	return handler(ctx, req)
}

// checks if this is one of the four namespace operations
func isNamespaceOp(api string) bool {
	_, ok := namespaceAPIs[api]
	return ok
}

type interceptor struct {
	authorizer    Authorizer
	metricsClient metrics.Client
}

// GetAuthorizationInterceptor creates an authorization interceptor and return a func that points to its Interceptor method
func NewAuthorizationInterceptor(authorizer Authorizer, metrics metrics.Client) grpc.UnaryServerInterceptor {
	return (&interceptor{authorizer: authorizer, metricsClient: metrics}).Interceptor
}

// getMetricsScopeWithNamespace return metrics scope with namespace tag
func (a *interceptor) getMetricsScope(
	scope int,
	namespace string,
) metrics.Scope {
	var metricsScope metrics.Scope
	if namespace != "" {
		metricsScope = a.metricsClient.Scope(scope).Tagged(metrics.NamespaceTag(namespace))
	} else {
		metricsScope = a.metricsClient.Scope(scope).Tagged(metrics.NamespaceUnknownTag())
	}
	return metricsScope
}
