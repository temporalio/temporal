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

package metrics

import (
	"reflect"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

const (
	grpcWorkflowService = "/temporal.api.workflowservice.v1.WorkflowService"
	grpcOperatorService = "/temporal.api.operatorservice.v1.OperatorService"
	grpcAdminService    = "/temporal.server.api.adminservice.v1.AdminService"
	grpcMatchingService = "/temporal.server.api.matchingservice.v1.MatchingService"
	grpcHistoryService  = "/temporal.server.api.historyservice.v1.HistoryService"
)

func getAPINames(svcType reflect.Type, grpcSvcName string) map[string]string {
	apiNames := make(map[string]string, svcType.NumMethod())
	for i := 0; i < svcType.NumMethod(); i++ {
		methodName := svcType.Method(i).Name
		apiNames[methodName] = grpcSvcName + "/" + methodName
	}
	return apiNames
}

func frontendAPIMetricsNames() map[string]string {
	apiNames := getAPINames(reflect.TypeOf((*workflowservice.WorkflowServiceServer)(nil)).Elem(), grpcWorkflowService)
	for methodName, fullName := range getAPINames(reflect.TypeOf((*adminservice.AdminServiceServer)(nil)).Elem(), grpcAdminService) {
		apiNames["Admin"+methodName] = fullName
	}
	for methodName, fullName := range getAPINames(reflect.TypeOf((*operatorservice.OperatorServiceServer)(nil)).Elem(), grpcOperatorService) {
		apiNames["Operator"+methodName] = fullName
	}
	return apiNames
}

func matchingAPIMetricsNames() map[string]string {
	return getAPINames(reflect.TypeOf((*matchingservice.MatchingServiceServer)(nil)).Elem(), grpcMatchingService)
}

func historyAPIMetricsNames() map[string]string {
	return getAPINames(reflect.TypeOf((*historyservice.HistoryServiceServer)(nil)).Elem(), grpcHistoryService)
}

func getAPIMetricsScopes(apiNames map[string]string, scopeDefs map[int]scopeDefinition) map[string]int {
	apiNameToScope := make(map[string]int, len(apiNames))
	for scope, name := range scopeDefs {
		if fullName, ok := apiNames[name.operation]; ok {
			apiNameToScope[fullName] = scope
		}
	}
	return apiNameToScope
}

func FrontendAPIMetricsScopes() map[string]int {
	return getAPIMetricsScopes(frontendAPIMetricsNames(), ScopeDefs[Frontend])
}

func MatchingAPIMetricsScopes() map[string]int {
	return getAPIMetricsScopes(matchingAPIMetricsNames(), ScopeDefs[Matching])
}

func HistoryAPIMetricsScopes() map[string]int {
	return getAPIMetricsScopes(historyAPIMetricsNames(), ScopeDefs[History])
}
