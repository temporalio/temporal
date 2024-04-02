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

package nexus_test

import (
	"fmt"

	"go.temporal.io/server/common/nexus"
)

func ExampleRouteSet_DispatchNexusTaskByNamespaceAndTaskQueue() {
	path := nexus.Routes().DispatchNexusTaskByNamespaceAndTaskQueue.
		Path(nexus.NamespaceAndTaskQueue{
			Namespace: "TEST-NAMESPACE",
			TaskQueue: "TEST-TASK-QUEUE",
		})
	fmt.Println(path)
	// Output: api/v1/namespaces/TEST-NAMESPACE/task-queues/TEST-TASK-QUEUE/nexus-operations
}

func ExampleRouteSet_DispatchNexusTaskByService() {
	path := nexus.Routes().DispatchNexusTaskByService.
		Path("TEST-SERVICE")
	fmt.Println(path)
	// Output: api/v1/nexus/services/TEST-SERVICE/operations
}
