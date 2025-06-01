package api

import (
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
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
	return token.Marshal()
}

// NOTE: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/frontend/token_deprecated.go
func DeserializeRawHistoryToken(bytes []byte) (*tokenspb.RawHistoryContinuation, error) {
	token := &tokenspb.RawHistoryContinuation{}
	err := token.Unmarshal(bytes)
	return token, err
}

// NOTE: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/frontend/token_deprecated.go
func SerializeHistoryToken(token *tokenspb.HistoryContinuation) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	return token.Marshal()
}

// NOTE: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/frontend/token_deprecated.go
func DeserializeHistoryToken(bytes []byte) (*tokenspb.HistoryContinuation, error) {
	token := &tokenspb.HistoryContinuation{}
	err := token.Unmarshal(bytes)
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
