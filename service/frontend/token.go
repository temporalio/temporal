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

package frontend

import (
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
)

func generatePaginationToken(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *historyspb.VersionHistories,
) *tokenspb.RawHistoryContinuation {

	execution := request.Execution
	return &tokenspb.RawHistoryContinuation{
		Namespace:         request.GetNamespace(),
		NamespaceId:       request.GetNamespaceId(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		StartEventId:      request.GetStartEventId(),
		StartEventVersion: request.GetStartEventVersion(),
		EndEventId:        request.GetEndEventId(),
		EndEventVersion:   request.GetEndEventVersion(),
		VersionHistories:  versionHistories,
		PersistenceToken:  nil, // this is the initialized value
	}
}

func validatePaginationToken(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	token *tokenspb.RawHistoryContinuation,
) error {

	execution := request.Execution
	if (request.GetNamespaceId() != token.GetNamespaceId() && request.GetNamespace() != token.GetNamespace()) ||
		execution.GetWorkflowId() != token.GetWorkflowId() ||
		execution.GetRunId() != token.GetRunId() ||
		request.GetStartEventId() != token.GetStartEventId() ||
		request.GetStartEventVersion() != token.GetStartEventVersion() ||
		request.GetEndEventId() != token.GetEndEventId() ||
		request.GetEndEventVersion() != token.GetEndEventVersion() {
		return errInvalidPaginationToken
	}
	return nil
}

func serializeRawHistoryToken(token *tokenspb.RawHistoryContinuation) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	return token.Marshal()
}

func deserializeRawHistoryToken(bytes []byte) (*tokenspb.RawHistoryContinuation, error) {
	token := &tokenspb.RawHistoryContinuation{}
	err := token.Unmarshal(bytes)
	return token, err
}

func serializeHistoryToken(token *tokenspb.HistoryContinuation) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	return token.Marshal()
}

func deserializeHistoryToken(bytes []byte) (*tokenspb.HistoryContinuation, error) {
	token := &tokenspb.HistoryContinuation{}
	err := token.Unmarshal(bytes)
	return token, err
}
