// The MIT License
//
// Copyright (c) 2022 Temporal Technologies Inc.  All rights reserved.
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

package batcher

import (
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	commonpb "go.temporal.io/api/common/v1"
)

func TestEncodeOperationJobID(t *testing.T) {
	execution := &commonpb.WorkflowExecution{
		WorkflowId: uuid.New(),
		RunId:      uuid.New(),
	}

	jobID, err := EncodeOperationJobID(execution)
	assert.NoError(t, err)
	decodedExecution, err := DecodeOperationJobID(jobID)
	assert.NoError(t, err)
	assert.Equal(t, execution, decodedExecution)
}

func TestGenerateOperationID_Termination(t *testing.T) {
	input := BatchParams{
		Namespace:       uuid.New(),
		Query:           "query",
		Reason:          "test",
		BatchType:       "terminate",
		TerminateParams: TerminateParams{},
		CancelParams:    CancelParams{},
		SignalParams:    SignalParams{},
	}

	id := GenerateOperationID(input)
	assert.Equal(t, 64, len(id))
}

func TestGenerateOperationID_Signal(t *testing.T) {
	input := BatchParams{
		Namespace:       uuid.New(),
		Query:           "query",
		Reason:          "test",
		BatchType:       "signal",
		TerminateParams: TerminateParams{},
		CancelParams:    CancelParams{},
		SignalParams: SignalParams{
			SignalName: "SignalName",
			Input:      &commonpb.Payloads{},
		},
	}

	id := GenerateOperationID(input)
	fmt.Println(id)
	assert.Equal(t, 64, len(id))
}
