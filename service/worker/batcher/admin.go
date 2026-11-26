package batcher

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/adminservice/v1"
	batchspb "go.temporal.io/server/api/batch/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/sqlquery"
	workercommon "go.temporal.io/server/service/worker/common"
)

var errAdminBatchNamespaceNotSystem = errors.New("admin batches should run in the system namespace")

// ValidateAllNamespacesQuery rejects a visibility query that cannot be used across all namespaces:
// a blank query would select every execution in the cluster, and custom search attributes, including
// raw field names such as Keyword01, mean different things in different namespaces.
func ValidateAllNamespacesQuery(query string) error {
	if strings.TrimSpace(query) == "" {
		return errors.New("a query across all namespaces requires a visibility query")
	}
	statement, err := sqlparser.Parse(fmt.Sprintf(sqlquery.QueryTemplate, query))
	if err != nil {
		return fmt.Errorf("invalid query %q: %w", query, err)
	}
	var customSAs []string
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if col, ok := node.(*sqlparser.ColName); ok {
			name := strings.ReplaceAll(sqlparser.String(col), "`", "")
			if !isSystemOrPredefinedSearchAttribute(name) {
				customSAs = append(customSAs, name)
			}
		}
		return true, nil
	}, statement)
	if len(customSAs) > 0 {
		return fmt.Errorf("a query across all namespaces cannot filter on custom search attributes; only system and predefined search attributes are supported, got: %s",
			strings.Join(customSAs, ", "))
	}
	return nil
}

// isSystemOrPredefinedSearchAttribute accepts canonical names and reserved aliases.
// Unprefixed aliases can collide with custom fields, which take precedence during query resolution.
func isSystemOrPredefinedSearchAttribute(name string) bool {
	if !sadefs.IsMappable(name) {
		return true
	}
	trimmed, prefixed := strings.CutPrefix(name, sadefs.ReservedPrefix)
	return prefixed && (trimmed == sadefs.ScheduleID || !sadefs.IsMappable(trimmed))
}

func isAdminRequest(batchParams *batchspb.BatchOperationInput) bool {
	return batchParams.AdminRequest != nil
}

func isAdminDelegatedRequest(batchParams *batchspb.BatchOperationInput) bool {
	_, ok := batchParams.GetAdminRequest().GetOperation().(*adminservice.StartAdminBatchOperationRequest_DelegationOperation)
	return ok
}

func (a *activities) adjustQueryAdminBatchType(
	batchParams *batchspb.BatchOperationInput,
) string {
	adminReq := batchParams.GetAdminRequest()
	// if a user batch is delegated as an admin batch
	if isAdminDelegatedRequest(batchParams) {
		batchType := batchParams.BatchType
		return a.adjustQueryBatchTypeEnum(adminReq.GetVisibilityQuery(), batchType)
	}
	// other admin batch:
	// RefreshWorkflowTasks applies to both open and closed workflows,
	// so no additional filter is needed - return query as-is.
	return adminReq.GetVisibilityQuery()
}

// fetchAllNamespacesPage fetches the next page of an admin batch across all namespaces.
func fetchAllNamespacesPage(
	ctx context.Context,
	adminClient adminservice.AdminServiceClient,
	config batchProcessorConfig,
	pageToken []byte,
	pageNumber int,
) (*page, error) {
	resp, err := adminClient.ListExecutions(ctx, &adminservice.ListExecutionsRequest{
		Query:         config.adjustedQuery,
		PageSize:      int32(pageSize),
		NextPageToken: pageToken,
	})
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.InvalidArgument](err); ok {
			return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidArgument", err)
		}
		return nil, err
	}
	return &page{
		crossNamespaceExecutionInfos: resp.GetExecutions(),
		nextPageToken:                resp.GetNextPageToken(),
		pageNumber:                   pageNumber,
	}, nil
}

func (a *activities) processAdminTask(
	ctx context.Context,
	batchOperation *batchspb.BatchOperationInput,
	task task,
	limiter quotas.RequestRateLimiter,
) error {
	adminReq := batchOperation.AdminRequest
	switch adminReq.Operation.(type) {
	case *adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation:
		if info := task.crossNamespaceExecutionInfo; info != nil {
			return processTask(ctx, limiter, task,
				func(*workflowpb.WorkflowExecutionInfo) error {
					execution := &commonpb.WorkflowExecution{WorkflowId: info.GetBusinessId(), RunId: info.GetRunId()}
					return a.refreshWorkflowTasks(ctx, info.GetNamespaceId(), info.GetArchetypeId(), execution)
				})
		}
		return processTask(ctx, limiter, task,
			func(executionInfo *workflowpb.WorkflowExecutionInfo) error {
				archetypeID, err := workercommon.ArchetypeIDFromExecutionInfo(executionInfo)
				if err != nil {
					return fmt.Errorf("archetypeID extraction error: %w", err)
				}
				return a.refreshWorkflowTasks(ctx, batchOperation.NamespaceId, uint32(archetypeID), executionInfo.Execution)
			})
	default:
		return fmt.Errorf("unknown admin batch type: %T", adminReq.Operation)
	}
}

func (a *activities) refreshWorkflowTasks(
	ctx context.Context,
	namespaceID string,
	archetypeID uint32,
	execution *commonpb.WorkflowExecution,
) error {
	_, err := a.HistoryClient.RefreshWorkflowTasks(ctx, &historyservice.RefreshWorkflowTasksRequest{
		NamespaceId: namespaceID,
		ArchetypeId: archetypeID,
		Request: &adminservice.RefreshWorkflowTasksRequest{
			NamespaceId: namespaceID,
			Execution:   execution,
		},
	})
	return err
}

func allNamespaces(batchParams *batchspb.BatchOperationInput) bool {
	return batchParams.GetAdminRequest().GetRefreshTasksOperation().GetAllNamespaces()
}
