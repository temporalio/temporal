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

type NamespaceAndTaskQueue struct {
	Namespace string
	TaskQueue string
}

var RouteDispatchNexusTaskByNamespaceAndTaskQueue = routing.NewBuilder[NamespaceAndTaskQueue]().
	Constant("api", "v1", "namespaces").
	StringVariable("namespace", func(params *NamespaceAndTaskQueue) *string { return &params.Namespace }).
	Constant("task-queues").
	StringVariable("task_queue", func(params *NamespaceAndTaskQueue) *string { return &params.TaskQueue }).
	Constant("nexus-services").
	Build()

var RouteDispatchNexusTaskByEndpoint = routing.NewBuilder[string]().
	Constant("api", "v1", "nexus", "endpoints").
	StringVariable("endpoint", func(endpoint *string) *string { return endpoint }).
	Constant("services").
	Build()

// RouteCompletionCallback is an HTTP route for completing a Nexus operation via callback.
var RouteCompletionCallback = routing.NewBuilder[string]().
	Constant("api", "v1", "namespaces").
	StringVariable("namespace", func(namespace *string) *string { return namespace }).
	Constant("nexus", "callback").
	Build()
