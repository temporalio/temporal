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

package definition

type (
	// WorkflowKey is the combinations which represent a current workflow
	CurrentWorkflowKey struct {
		NamespaceID string
		WorkflowID  string
	}

	// WorkflowKey is the combinations which represent a workflow
	WorkflowKey struct {
		NamespaceID string
		WorkflowID  string
		RunID       string
	}
)

// NewCurrentWorkflowKey create a new CurrentWorkflowKey
func NewCurrentWorkflowKey(
	namespaceID string,
	workflowID string,
) CurrentWorkflowKey {
	return CurrentWorkflowKey{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}
}

// NewWorkflowKey create a new WorkflowKey
func NewWorkflowKey(
	namespaceID string,
	workflowID string,
	runID string,
) WorkflowKey {
	return WorkflowKey{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
}

func (k *WorkflowKey) GetNamespaceID() string {
	return k.NamespaceID
}

func (k *WorkflowKey) GetWorkflowID() string {
	return k.WorkflowID
}

func (k *WorkflowKey) GetRunID() string {
	return k.RunID
}
