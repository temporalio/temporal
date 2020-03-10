package frontend

import (
	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/token"
	"github.com/temporalio/temporal/common/persistence"
)

func generatePaginationToken(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *persistence.VersionHistories,
) *token.RawHistoryContinuationToken {

	execution := request.Execution
	return &token.RawHistoryContinuationToken{
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
	token *token.RawHistoryContinuationToken,
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

func serializeRawHistoryToken(token *token.RawHistoryContinuationToken) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	return token.Marshal()
}

func deserializeRawHistoryToken(bytes []byte) (*token.RawHistoryContinuationToken, error) {
	token := &token.RawHistoryContinuationToken{}
	err := token.Unmarshal(bytes)
	return token, err
}

func serializeHistoryToken(token *token.HistoryContinuationToken) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	return token.Marshal()
}

func deserializeHistoryToken(bytes []byte) (*token.HistoryContinuationToken, error) {
	token := &token.HistoryContinuationToken{}
	err := token.Unmarshal(bytes)
	return token, err
}
