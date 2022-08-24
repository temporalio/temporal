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
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
)

func EncodeOperationJobID(execution *commonpb.WorkflowExecution) (string, error) {
	data, err := execution.Marshal()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func DecodeOperationJobID(jobID string) (*commonpb.WorkflowExecution, error) {
	data, err := base64.StdEncoding.DecodeString(jobID)
	if err != nil {
		return nil, err
	}
	execution := &commonpb.WorkflowExecution{}
	err = execution.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	return execution, nil
}

func GenerateOperationID(input BatchParams) string {
	var operationIDFactor []string
	operationIDFactor = append(operationIDFactor, input.Query)
	operationIDFactor = append(operationIDFactor, input.Reason)
	operationIDFactor = append(operationIDFactor, input.BatchType)
	operationIDFactor = append(operationIDFactor, input.SignalParams.SignalName)
	if input.SignalParams.Input != nil {
		operationIDFactor = append(operationIDFactor, input.SignalParams.Input.String())
	}
	operationID := strings.Join(operationIDFactor, "")
	hashValue := sha256.Sum256([]byte(operationID))
	return fmt.Sprintf("%x", hashValue)
}
