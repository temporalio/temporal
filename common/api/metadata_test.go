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

package api

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"golang.org/x/exp/maps"
)

func TestWorkflowServiceMetadata(t *testing.T) {
	tp := reflect.TypeOf((*workflowservice.WorkflowServiceServer)(nil)).Elem()
	assert.ElementsMatch(t, getMethodNames(tp), maps.Keys(workflowServiceMetadata),
		"If you're adding a new method to WorkflowService, please add metadata for it in metadata.go")
}

func TestOperatorServiceMetadata(t *testing.T) {
	tp := reflect.TypeOf((*operatorservice.OperatorServiceServer)(nil)).Elem()
	assert.ElementsMatch(t, getMethodNames(tp), maps.Keys(operatorServiceMetadata),
		"If you're adding a new method to OperatorService, please add metadata for it in metadata.go")
}

func TestGetMethodMetadata(t *testing.T) {
	md := GetMethodMetadata("/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted")
	assert.Equal(t, ScopeNamespace, md.Scope)
	assert.Equal(t, AccessWrite, md.Access)

	// all AdminService is cluster/admin
	md = GetMethodMetadata("/temporal.server.api.adminservice.v1.AdminService/CloseShard")
	assert.Equal(t, ScopeCluster, md.Scope)
	assert.Equal(t, AccessAdmin, md.Access)

	md = GetMethodMetadata("/OtherService/Method1")
	assert.Equal(t, ScopeUnknown, md.Scope)
	assert.Equal(t, AccessUnknown, md.Access)
}

func getMethodNames(tp reflect.Type) []string {
	var out []string
	for i := 0; i < tp.NumMethod(); i++ {
		out = append(out, tp.Method(i).Name)
	}
	return out
}
