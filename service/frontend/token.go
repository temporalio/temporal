// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/token"
	"github.com/temporalio/temporal/common/persistence"
)

func generatePaginationToken(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *persistence.VersionHistories,
) *token.RawHistoryContinuation {

	execution := request.Execution
	return &token.RawHistoryContinuation{
		DomainName:        request.GetDomain(),
		WorkflowId:        execution.GetWorkflowId(),
		RunId:             execution.GetRunId(),
		StartEventId:      request.GetStartEventId(),
		StartEventVersion: request.GetStartEventVersion(),
		EndEventId:        request.GetEndEventId(),
		EndEventVersion:   request.GetEndEventVersion(),
		VersionHistories:  versionHistories.ToProto(),
		PersistenceToken:  nil, // this is the initialized value
	}
}

func validatePaginationToken(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	token *token.RawHistoryContinuation,
) error {

	execution := request.Execution
	if request.GetDomain() != token.GetDomainName() ||
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

func serializeRawHistoryToken(token *token.RawHistoryContinuation) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	return token.Marshal()
}

func deserializeRawHistoryToken(bytes []byte) (*token.RawHistoryContinuation, error) {
	token := &token.RawHistoryContinuation{}
	err := token.Unmarshal(bytes)
	return token, err
}

func serializeHistoryToken(token *token.HistoryContinuation) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	return token.Marshal()
}

func deserializeHistoryToken(bytes []byte) (*token.HistoryContinuation, error) {
	token := &token.HistoryContinuation{}
	err := token.Unmarshal(bytes)
	return token, err
}
