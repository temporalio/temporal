package frontend

import (
	"github.com/temporalio/temporal/.gen/proto/adminservice"
	tokengenpb "github.com/temporalio/temporal/.gen/proto/token"
	"github.com/temporalio/temporal/common/persistence"
)

func generatePaginationToken(
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *persistence.VersionHistories,
) *tokengenpb.RawHistoryContinuation {

	execution := request.Execution
	return &tokengenpb.RawHistoryContinuation{
		Namespace:         request.GetNamespace(),
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
	token *tokengenpb.RawHistoryContinuation,
) error {

	execution := request.Execution
	if request.GetNamespace() != token.GetNamespace() ||
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

func serializeRawHistoryToken(token *tokengenpb.RawHistoryContinuation) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	return token.Marshal()
}

func deserializeRawHistoryToken(bytes []byte) (*tokengenpb.RawHistoryContinuation, error) {
	token := &tokengenpb.RawHistoryContinuation{}
	err := token.Unmarshal(bytes)
	return token, err
}

func serializeHistoryToken(token *tokengenpb.HistoryContinuation) ([]byte, error) {
	if token == nil {
		return nil, nil
	}

	return token.Marshal()
}

func deserializeHistoryToken(bytes []byte) (*tokengenpb.HistoryContinuation, error) {
	token := &tokengenpb.HistoryContinuation{}
	err := token.Unmarshal(bytes)
	return token, err
}
