package localserver

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/workflow"
	"google.golang.org/protobuf/proto"
)

// Admin serves the AdminService methods a host needs to run, on the local server, a workflow run
// that a server owns: importing the run's history, reading the history the run added locally, and
// deleting the run when the server takes it back.
//
// All events have version 0, the version of every event in a namespace that is not global: the
// local server rejects history from global namespaces.
type Admin struct {
	s *Server
}

func (s *Server) Admin() Admin {
	return Admin{s: s}
}

// ImportWorkflowExecution creates a run from the history a server returns when a local server
// acquires a new run: its started event and its first scheduled workflow task, in any batches.
func (a Admin) ImportWorkflowExecution(
	ctx context.Context,
	request *adminservice.ImportWorkflowExecutionRequest,
) (*adminservice.ImportWorkflowExecutionResponse, error) {
	var events []*historypb.HistoryEvent
	for _, blob := range request.GetHistoryBatches() {
		batch, err := decodeHistoryBatch(blob)
		if err != nil {
			return nil, err
		}
		events = append(events, batch...)
	}
	for _, event := range events {
		if event.GetVersion() != 0 {
			return nil, serviceerror.NewInvalidArgument("history from a global namespace cannot be imported")
		}
	}
	_, err := chasm.StartExecution(
		a.s.ctx(ctx),
		executionKey(request.GetNamespace(), request.GetExecution()),
		workflow.NewImportedNativeWorkflow,
		events,
	)
	if err != nil {
		return nil, err
	}
	return &adminservice.ImportWorkflowExecutionResponse{}, nil
}

// GetWorkflowExecutionRawHistoryV2 returns, in one page, the history batches after the start event
// and a version history ending at the last event. It ignores the end event and page size.
func (a Admin) GetWorkflowExecutionRawHistoryV2(
	ctx context.Context,
	request *adminservice.GetWorkflowExecutionRawHistoryV2Request,
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
	ref := chasm.NewComponentRef[*workflow.Workflow](executionKey(request.GetNamespaceId(), request.GetExecution()))
	return chasm.ReadComponent(a.s.ctx(ctx), ref, func(
		w *workflow.Workflow,
		ctx chasm.Context,
		startEventID int64,
	) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
		response := &adminservice.GetWorkflowExecutionRawHistoryV2Response{}
		for _, batch := range w.HistoryBatchesAfter(ctx, startEventID) {
			data, err := proto.Marshal(&historypb.History{Events: batch})
			if err != nil {
				return nil, err
			}
			response.HistoryBatches = append(response.HistoryBatches, &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         data,
			})
		}
		history := w.History(ctx)
		response.VersionHistory = &historyspb.VersionHistory{Items: []*historyspb.VersionHistoryItem{
			{EventId: history[len(history)-1].GetEventId()},
		}}
		return response, nil
	}, request.GetStartEventId())
}

// DeleteWorkflowExecution removes a run, running or not.
func (a Admin) DeleteWorkflowExecution(
	_ context.Context,
	request *adminservice.DeleteWorkflowExecutionRequest,
) (*adminservice.DeleteWorkflowExecutionResponse, error) {
	x, err := a.s.engine.execution(chasm.ComponentRef{ExecutionKey: executionKey(request.GetNamespace(), request.GetExecution())})
	if err != nil {
		return nil, err
	}
	a.s.engine.deleteExecution(x)
	return &adminservice.DeleteWorkflowExecutionResponse{}, nil
}

func executionKey(namespace string, execution *commonpb.WorkflowExecution) chasm.ExecutionKey {
	return chasm.ExecutionKey{NamespaceID: namespace, BusinessID: execution.GetWorkflowId(), RunID: execution.GetRunId()}
}

func decodeHistoryBatch(blob *commonpb.DataBlob) ([]*historypb.HistoryEvent, error) {
	if blob.GetEncodingType() != enumspb.ENCODING_TYPE_PROTO3 {
		return nil, serviceerror.NewInvalidArgumentf("unsupported history encoding %v", blob.GetEncodingType())
	}
	history := &historypb.History{}
	if err := proto.Unmarshal(blob.GetData(), history); err != nil {
		return nil, serviceerror.NewInvalidArgument(err.Error())
	}
	return history.GetEvents(), nil
}
