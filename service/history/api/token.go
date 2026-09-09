package api

import (
	"bytes"
	"context"

	commonpb "go.temporal.io/api/common/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
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

const (
	// branchTokenMismatchReasonNonCurrent is a branch this execution records but no longer reads from.
	branchTokenMismatchReasonNonCurrent metrics.ReasonString = "non_current_branch"
	branchTokenMismatchReasonForeign    metrics.ReasonString = "foreign_branch"
)

// maxLoggedBranchTokenLen bounds caller-supplied bytes reaching the log.
const maxLoggedBranchTokenLen = 4096

func branchTokenMismatchReason(
	currentBranchToken []byte,
	requestBranchToken []byte,
	versionHistories *historyspb.VersionHistories,
) metrics.ReasonString {
	if bytes.Equal(requestBranchToken, currentBranchToken) {
		return ""
	}
	for _, versionHistory := range versionHistories.GetHistories() {
		if bytes.Equal(versionHistory.GetBranchToken(), requestBranchToken) {
			return branchTokenMismatchReasonNonCurrent
		}
	}
	return branchTokenMismatchReasonForeign
}

func reportBranchTokenMismatch(
	shardContext historyi.ShardContext,
	namespaceName string,
	execution *commonpb.WorkflowExecution,
	reason metrics.ReasonString,
	currentBranchToken []byte,
	requestBranchToken []byte,
) {
	if namespaceName == "" {
		namespaceName = metrics.NamespaceUnknownTag().Value
	}
	metrics.PaginationTokenBranchMismatchCounter.With(shardContext.GetMetricsHandler()).Record(
		1,
		metrics.NamespaceTag(namespaceName),
		metrics.ReasonTag(reason),
	)
	loggedRequestToken := requestBranchToken[:min(len(requestBranchToken), maxLoggedBranchTokenLen)]
	shardContext.GetLogger().Warn("Pagination branch token is not the execution's current branch token",
		tag.WorkflowNamespace(namespaceName),
		tag.WorkflowID(execution.GetWorkflowId()),
		tag.WorkflowRunID(execution.GetRunId()),
		tag.NewStringTag("reason", string(reason)),
		tag.WorkflowBranchToken(currentBranchToken),
		tag.WorkflowRequestBranchToken(loggedRequestToken),
	)
}

// ValidateBranchTokenForExecution rejects a paging branch token that is not the execution's current
// one.
func ValidateBranchTokenForExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker WorkflowConsistencyChecker,
	eventNotifier events.Notifier,
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	requestBranchToken []byte,
) error {
	config := shardContext.GetConfig()
	if !config.EnablePaginationTokenBranchValidation() {
		return nil
	}
	if len(requestBranchToken) == 0 {
		return consts.ErrInvalidNextPageToken
	}

	response, err := GetOrPollWorkflowMutableState(
		ctx,
		shardContext,
		&historyservice.GetMutableStateRequest{
			NamespaceId: namespaceID.String(),
			Execution:   execution,
		},
		workflowConsistencyChecker,
		eventNotifier,
	)
	if err != nil {
		return err
	}

	currentBranchToken := response.GetCurrentBranchToken()
	mismatchReason := branchTokenMismatchReason(
		currentBranchToken,
		requestBranchToken,
		response.GetVersionHistories(),
	)
	if mismatchReason == "" {
		return nil
	}

	reportBranchTokenMismatch(
		shardContext,
		namespaceName.String(),
		execution,
		mismatchReason,
		currentBranchToken,
		requestBranchToken,
	)
	if config.EnablePaginationTokenBranchValidationShadowMode() {
		return nil
	}
	return serviceerrors.NewCurrentBranchChanged(
		currentBranchToken,
		requestBranchToken,
		nil,
		nil,
	)
}
