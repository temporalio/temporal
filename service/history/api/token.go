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
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/utf8validator"
	"go.temporal.io/server/service/history/consts"
)

// NOTE: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/frontend/token_deprecated.go
func GeneratePaginationTokenV2Request(
	request *historyservice.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *historyspb.VersionHistories,
) *tokenspb.RawHistoryContinuation {

	req := request.Request
	execution := req.Execution
	return &tokenspb.RawHistoryContinuation{
		NamespaceId:       req.GetNamespaceId(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		StartEventId:      req.GetStartEventId(),
		StartEventVersion: req.GetStartEventVersion(),
		EndEventId:        req.GetEndEventId(),
		EndEventVersion:   req.GetEndEventVersion(),
		VersionHistories:  versionHistories,
		PersistenceToken:  nil, // this is the initialized value
	}
}

// NOTE: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/frontend/token_deprecated.go
func ValidatePaginationTokenV2Request(
	request *historyservice.GetWorkflowExecutionRawHistoryV2Request,
	token *tokenspb.RawHistoryContinuation,
) error {

	req := request.Request
	execution := req.Execution
	if req.GetNamespaceId() != token.GetNamespaceId() ||
		execution.GetWorkflowId() != token.GetWorkflowId() ||
		execution.GetRunId() != token.GetRunId() ||
		req.GetStartEventId() != token.GetStartEventId() ||
		req.GetStartEventVersion() != token.GetStartEventVersion() ||
		req.GetEndEventId() != token.GetEndEventId() ||
		req.GetEndEventVersion() != token.GetEndEventVersion() {
		return consts.ErrInvalidPaginationToken
	}
	return nil
}

// NOTE: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/frontend/token_deprecated.go
func SerializeRawHistoryToken(token *tokenspb.RawHistoryContinuation) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	if err := utf8validator.Validate(token, utf8validator.SourceRPCResponse); err != nil {
		return nil, err
	}
	return token.Marshal()
}

// NOTE: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/frontend/token_deprecated.go
func DeserializeRawHistoryToken(bytes []byte) (*tokenspb.RawHistoryContinuation, error) {
	token := &tokenspb.RawHistoryContinuation{}
	err := token.Unmarshal(bytes)
	if err == nil {
		err = utf8validator.Validate(token, utf8validator.SourceRPCRequest)
	}
	return token, err
}

// NOTE: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/frontend/token_deprecated.go
func SerializeHistoryToken(token *tokenspb.HistoryContinuation) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	if err := utf8validator.Validate(token, utf8validator.SourceRPCResponse); err != nil {
		return nil, err
	}
	return token.Marshal()
}

// NOTE: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/frontend/token_deprecated.go
func DeserializeHistoryToken(bytes []byte) (*tokenspb.HistoryContinuation, error) {
	token := &tokenspb.HistoryContinuation{}
	err := token.Unmarshal(bytes)
	if err == nil {
		err = utf8validator.Validate(token, utf8validator.SourceRPCRequest)
	}
	return token, err
}

func GeneratePaginationToken(
	request *historyservice.GetWorkflowExecutionRawHistoryRequest,
	versionHistories *historyspb.VersionHistories,
) *tokenspb.RawHistoryContinuation {

	req := request.Request
	execution := req.Execution
	return &tokenspb.RawHistoryContinuation{
		NamespaceId:       req.GetNamespaceId(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		StartEventId:      req.GetStartEventId(),
		StartEventVersion: req.GetStartEventVersion(),
		EndEventId:        req.GetEndEventId(),
		EndEventVersion:   req.GetEndEventVersion(),
		VersionHistories:  versionHistories,
		PersistenceToken:  nil, // this is the initialized value
	}
}

func ValidatePaginationToken(
	request *historyservice.GetWorkflowExecutionRawHistoryRequest,
	token *tokenspb.RawHistoryContinuation,
) error {

	req := request.Request
	execution := req.Execution
	if req.GetNamespaceId() != token.GetNamespaceId() ||
		execution.GetWorkflowId() != token.GetWorkflowId() ||
		execution.GetRunId() != token.GetRunId() ||
		req.GetStartEventId() != token.GetStartEventId() ||
		req.GetStartEventVersion() != token.GetStartEventVersion() ||
		req.GetEndEventId() != token.GetEndEventId() ||
		req.GetEndEventVersion() != token.GetEndEventVersion() {
		return consts.ErrInvalidPaginationToken
	}
	return nil
}
