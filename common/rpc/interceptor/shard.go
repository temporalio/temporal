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
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
)

type (
	WorkflowExecutionGetter interface {
		GetExecution() *commonpb.WorkflowExecution
	}
)

func MustGetShardID(
	namespaceRegistry namespace.Registry,
	req interface{},
	numShards int32,
) int32 {
	shardID, err := GetShardID(namespaceRegistry, req, numShards)
	if err != nil {
		return -1
	}
	return shardID
}

func GetShardID(
	namespaceRegistry namespace.Registry,
	req interface{},
	numShards int32,
) (int32, error) {
	namespaceID, err := GetNamespaceID(namespaceRegistry, req)
	if err != nil {
		return -1, err
	}

	switch request := req.(type) {
	case WorkflowExecutionGetter:
		workflowID := request.GetExecution().GetWorkflowId()
		return common.WorkflowIDToHistoryShard(namespaceID.String(), workflowID, numShards), nil

	default:
		return -1, serviceerror.NewInternal(fmt.Sprintf("unable to extract workflow info from request: %+v", req))
	}
}
