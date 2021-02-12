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

	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

func FrontendAPIMetricsNames() map[string]struct{} {
	var service workflowservice.WorkflowServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiNames := make(map[string]struct{}, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiNames[t.Method(i).Name] = struct{}{}
	}
	return apiNames
}

func MatchingAPIMetricsNames() map[string]struct{} {
	var service matchingservice.MatchingServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiNames := make(map[string]struct{}, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiNames[t.Method(i).Name] = struct{}{}
	}
	return apiNames
}

func HistoryAPIMetricsNames() map[string]struct{} {
	var service historyservice.HistoryServiceServer
	t := reflect.TypeOf(&service).Elem()
	apiNames := make(map[string]struct{}, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		apiNames[t.Method(i).Name] = struct{}{}
	}
	return apiNames
}

func FrontendAPIMetricsScopes() map[string]int {
	apiNames := FrontendAPIMetricsNames()
	apiNameToScope := make(map[string]int, len(apiNames))
	for scope, name := range ScopeDefs[Frontend] {
		if _, ok := apiNames[name.operation]; ok {
			apiNameToScope[name.operation] = scope
		}
	}
	return apiNameToScope
}

func MatchingAPIMetricsScopes() map[string]int {
	apiNames := MatchingAPIMetricsNames()
	apiNameToScope := make(map[string]int, len(apiNames))
	for scope, name := range ScopeDefs[Matching] {
		if _, ok := apiNames[name.operation]; ok {
			apiNameToScope[name.operation] = scope
		}
	}
	return apiNameToScope
}

func HistoryAPIMetricsScopes() map[string]int {
	apiNames := HistoryAPIMetricsNames()
	apiNameToScope := make(map[string]int, len(apiNames))
	for scope, name := range ScopeDefs[History] {
		if _, ok := apiNames[name.operation]; ok {
			apiNameToScope[name.operation] = scope
		}
	}
	return apiNameToScope
}
