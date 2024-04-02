// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexus

import "go.temporal.io/server/common/routing"

// Routes returns a RouteSet for the Nexus HTTP API. These routes can be used by both the server and clients for
// type-safe URL construction and parsing. It's a function instead of a variable so that the underlying
// set cannot be modified by the caller.
func Routes() RouteSet {
	return routes
}

type RouteSet struct {
	DispatchNexusTaskByNamespaceAndTaskQueue routing.Route[NamespaceAndTaskQueue]
	DispatchNexusTaskByService               routing.Route[string]
}

type NamespaceAndTaskQueue struct {
	Namespace string
	TaskQueue string
}

var routes = RouteSet{
	DispatchNexusTaskByNamespaceAndTaskQueue: routing.NewBuilder[NamespaceAndTaskQueue]().
		Constant("api", "v1", "namespaces").
		StringVariable("namespace", func(params *NamespaceAndTaskQueue) *string { return &params.Namespace }).
		Constant("task-queues").
		StringVariable("task_queue", func(params *NamespaceAndTaskQueue) *string { return &params.TaskQueue }).
		Constant("nexus-operations").
		Build(),
	DispatchNexusTaskByService: routing.NewBuilder[string]().
		Constant("api", "v1", "nexus", "services").
		StringVariable("service", func(service *string) *string { return service }).
		Constant("operations").
		Build(),
}
