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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination resetor_mock.go

package reset

import (
	"context"

	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/service/history/execution"
)

type (
	// WorkflowResetor is deprecated: use WorkflowResetter instead when NDC is applies to all workflows
	WorkflowResetor interface {
		ResetWorkflowExecution(
			ctx context.Context,
			resetRequest *workflow.ResetWorkflowExecutionRequest,
			baseContext execution.Context,
			baseMutableState execution.MutableState,
			currContext execution.Context,
			currMutableState execution.MutableState,
		) (response *workflow.ResetWorkflowExecutionResponse, retError error)

		ApplyResetEvent(
			ctx context.Context,
			request *h.ReplicateEventsRequest,
			domainID, workflowID, currentRunID string,
		) (retError error)
	}
)
