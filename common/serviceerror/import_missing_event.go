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

package serviceerror

import (
	"github.com/gogo/status"
)

type (
	// ImportMissingEvent represents import missing event error.
	ImportMissingEvent struct {
		Message           string
		NamespaceId       string
		WorkflowId        string
		RunId             string
		StartEventId      int64
		StartEventVersion int64
		EndEventId        int64
		EndEventVersion   int64
		st                *status.Status
	}
)

// NewImportMissingEvent returns new ImportMissingEvent error.
func NewImportMissingEvent(
	message string,
	namespaceId string,
	workflowId string,
	runId string,
	startEventId int64,
	startEventVersion int64,
	endEventId int64,
	endEventVersion int64,
) error {
	return &ImportMissingEvent{
		Message:           message,
		NamespaceId:       namespaceId,
		WorkflowId:        workflowId,
		RunId:             runId,
		StartEventId:      startEventId,
		StartEventVersion: startEventVersion,
		EndEventId:        endEventId,
		EndEventVersion:   endEventVersion,
	}
}

// Error returns string message.
func (e *ImportMissingEvent) Error() string {
	return e.Message
}
