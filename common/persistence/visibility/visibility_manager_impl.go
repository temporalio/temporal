package visibility

import (
	"context"
	"fmt"
	"strconv"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// visibilityManagerImpl is responsible for:
	//  - convert request (serialized some fields),
	//  - call underlying store (standard or advanced),
	//  - convert response.
	visibilityManagerImpl struct {
		store  store.VisibilityStore
		logger log.Logger
	}
)

const (
	// MemoEncoding is default encoding for visibility memo.
	MemoEncoding = enumspb.ENCODING_TYPE_PROTO3
)

var _ manager.VisibilityManager = (*visibilityManagerImpl)(nil)

func newVisibilityManagerImpl(
	store store.VisibilityStore,
	logger log.Logger,
) *visibilityManagerImpl {
	return &visibilityManagerImpl{
		store:  store,
		logger: logger,
	}
}

func (p *visibilityManagerImpl) Close() {
	p.store.Close()
}

func (p *visibilityManagerImpl) GetReadStoreName(_ namespace.Name) string {
	return p.store.GetName()
}

func (p *visibilityManagerImpl) GetStoreNames() []string {
	return []string{p.store.GetName()}
}

func (p *visibilityManagerImpl) HasStoreName(stName string) bool {
	return p.store.GetName() == stName
}

func (p *visibilityManagerImpl) GetIndexName() string {
	return p.store.GetIndexName()
}

func (p *visibilityManagerImpl) ValidateCustomSearchAttributes(
	searchAttributes map[string]any,
) (map[string]any, error) {
	return p.store.ValidateCustomSearchAttributes(searchAttributes)
}

func (p *visibilityManagerImpl) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionStartedRequest,
) error {
	requestBase, err := p.newInternalVisibilityRequestBase(request.VisibilityRequestBase)
	if err != nil {
		return err
	}
	req := &store.InternalRecordWorkflowExecutionStartedRequest{
		InternalVisibilityRequestBase: requestBase,
	}
	return p.store.RecordWorkflowExecutionStarted(ctx, req)
}

func (p *visibilityManagerImpl) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *manager.RecordWorkflowExecutionClosedRequest,
) error {
	requestBase, err := p.newInternalVisibilityRequestBase(request.VisibilityRequestBase)
	if err != nil {
		return err
	}
	req := &store.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: requestBase,
		CloseTime:                     request.CloseTime,
		HistoryLength:                 request.HistoryLength,
		HistorySizeBytes:              request.HistorySizeBytes,
		ExecutionDuration:             request.ExecutionDuration,
		StateTransitionCount:          request.StateTransitionCount,
	}
	return p.store.RecordWorkflowExecutionClosed(ctx, req)
}

func (p *visibilityManagerImpl) UpsertWorkflowExecution(
	ctx context.Context,
	request *manager.UpsertWorkflowExecutionRequest,
) error {
	requestBase, err := p.newInternalVisibilityRequestBase(request.VisibilityRequestBase)
	if err != nil {
		return err
	}
	req := &store.InternalUpsertWorkflowExecutionRequest{
		InternalVisibilityRequestBase: requestBase,
	}
	return p.store.UpsertWorkflowExecution(ctx, req)
}

func (p *visibilityManagerImpl) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	return p.store.DeleteWorkflowExecution(ctx, request)
}

func (p *visibilityManagerImpl) ListWorkflowExecutions(
	ctx context.Context,
	request *manager.ListWorkflowExecutionsRequestV2,
) (*manager.ListWorkflowExecutionsResponse, error) {
	response, err := p.store.ListWorkflowExecutions(ctx, request)
	if err != nil {
		return nil, err
	}

	return p.convertInternalListResponse(response)
}

func (p *visibilityManagerImpl) ListChasmExecutions(
	ctx context.Context,
	request *manager.ListChasmExecutionsRequest,
) (*chasm.ListExecutionsResponse[*commonpb.Payload], error) {
	response, err := p.store.ListChasmExecutions(ctx, request)
	if err != nil {
		return nil, err
	}
	executions := make([]*chasm.ExecutionInfo[*commonpb.Payload], 0, len(response.Executions))
	for _, exec := range response.Executions {
		combinedMemo, err := deserializeMemo(exec.Memo)
		if err != nil {
			return nil, err
		}

		var userMemo *commonpb.Memo
		var chasmMemoPayload *commonpb.Payload

		// Check if archetype exists to decide how to split memo, assume any number indicates an archetype id.
		archetypeExists := false
		if archetypePayload, ok := exec.SearchAttributes.GetIndexedFields()[sadefs.TemporalNamespaceDivision]; ok {
			var archetypeIDStr string
			if err := payload.Decode(archetypePayload, &archetypeIDStr); err == nil {
				if _, err := strconv.Atoi(archetypeIDStr); err == nil {
					archetypeExists = true
				}
			}
		}

		if archetypeExists && combinedMemo != nil {
			// Archetype matches - split memo into user and chasm parts
			userPayload := combinedMemo.Fields[chasm.UserMemoKey]
			if err := payload.Decode(userPayload, &userMemo); err != nil {
				p.logger.Error("failed to decode user memo", tag.Error(err))
				userMemo = nil
			}
			chasmMemoPayload = combinedMemo.Fields[chasm.ChasmMemoKey]
		} else {
			// Archetype doesn't match or no combined memo - return entire memo as user memo
			userMemo = combinedMemo
			chasmMemoPayload = nil
		}

		executions = append(executions, &chasm.ExecutionInfo[*commonpb.Payload]{
			BusinessID:             exec.WorkflowID,
			RunID:                  exec.RunID,
			StartTime:              exec.StartTime,
			CloseTime:              exec.CloseTime,
			HistoryLength:          exec.HistoryLength,
			HistorySizeBytes:       exec.HistorySizeBytes,
			StateTransitionCount:   exec.StateTransitionCount,
			ChasmSearchAttributes:  chasm.NewSearchAttributesMap(exec.ChasmSearchAttributes),
			CustomSearchAttributes: exec.SearchAttributes.GetIndexedFields(),
			Memo:                   userMemo,
			ChasmMemo:              chasmMemoPayload,
		})
	}

	return &chasm.ListExecutionsResponse[*commonpb.Payload]{
		Executions:    executions,
		NextPageToken: response.NextPageToken,
	}, nil
}

func (p *visibilityManagerImpl) CountChasmExecutions(
	ctx context.Context,
	request *manager.CountChasmExecutionsRequest,
) (*chasm.CountExecutionsResponse, error) {
	response, err := p.store.CountChasmExecutions(ctx, request)
	if err != nil {
		return nil, err
	}

	return &chasm.CountExecutionsResponse{Count: response.Count}, nil
}

func (p *visibilityManagerImpl) CountWorkflowExecutions(
	ctx context.Context,
	request *manager.CountWorkflowExecutionsRequest,
) (*manager.CountWorkflowExecutionsResponse, error) {
	response, err := p.store.CountWorkflowExecutions(ctx, request)
	if err != nil {
		return nil, err
	}

	return response, err
}

func (p *visibilityManagerImpl) GetWorkflowExecution(
	ctx context.Context,
	request *manager.GetWorkflowExecutionRequest,
) (*manager.GetWorkflowExecutionResponse, error) {
	response, err := p.store.GetWorkflowExecution(ctx, request)
	if err != nil {
		return nil, err
	}
	execution, err := p.convertInternalWorkflowExecutionInfo(response.Execution)
	if err != nil {
		return nil, err
	}
	return &manager.GetWorkflowExecutionResponse{Execution: execution}, err
}

func (p *visibilityManagerImpl) AddSearchAttributes(
	ctx context.Context,
	request *manager.AddSearchAttributesRequest,
) error {
	return p.store.AddSearchAttributes(ctx, request)
}

func (p *visibilityManagerImpl) newInternalVisibilityRequestBase(
	request *manager.VisibilityRequestBase,
) (*store.InternalVisibilityRequestBase, error) {
	if request == nil {
		return nil, nil
	}
	memoBlob, err := serializeMemo(request.Memo)
	if err != nil {
		return nil, err
	}

	var searchAttrs *commonpb.SearchAttributes
	if len(request.SearchAttributes.GetIndexedFields()) > 0 {
		// Remove any system search attribute from the map.
		// This is necessary because the validation can supress errors when trying
		// to set a value on a system search attribute.
		searchAttrs = &commonpb.SearchAttributes{
			IndexedFields: make(map[string]*commonpb.Payload),
		}
		for key, value := range request.SearchAttributes.IndexedFields {
			if !sadefs.IsSystem(key) {
				searchAttrs.IndexedFields[key] = value
			}
		}
	}

	var (
		parentWorkflowID *string
		parentRunID      *string
	)
	if request.ParentExecution != nil {
		parentWorkflowID = &request.ParentExecution.WorkflowId
		parentRunID = &request.ParentExecution.RunId
	}

	return &store.InternalVisibilityRequestBase{
		NamespaceID:      request.NamespaceID.String(),
		WorkflowID:       request.Execution.GetWorkflowId(),
		RunID:            request.Execution.GetRunId(),
		WorkflowTypeName: request.WorkflowTypeName,
		StartTime:        request.StartTime,
		Status:           request.Status,
		ExecutionTime:    request.ExecutionTime,
		TaskID:           request.TaskID,
		ShardID:          request.ShardID,
		TaskQueue:        request.TaskQueue,
		Memo:             memoBlob,
		SearchAttributes: searchAttrs,
		ParentWorkflowID: parentWorkflowID,
		ParentRunID:      parentRunID,
		RootWorkflowID:   request.RootExecution.GetWorkflowId(),
		RootRunID:        request.RootExecution.GetRunId(),
	}, nil
}

func (p *visibilityManagerImpl) convertInternalListResponse(
	internalResponse *store.InternalListWorkflowExecutionsResponse,
) (*manager.ListWorkflowExecutionsResponse, error) {
	if internalResponse == nil {
		return nil, nil
	}

	resp := &manager.ListWorkflowExecutionsResponse{}
	resp.Executions = make([]*workflowpb.WorkflowExecutionInfo, len(internalResponse.Executions))
	for i, execution := range internalResponse.Executions {
		var err error
		resp.Executions[i], err = p.convertInternalWorkflowExecutionInfo(execution)
		if err != nil {
			return nil, err
		}
	}

	resp.NextPageToken = internalResponse.NextPageToken
	return resp, nil
}

func (p *visibilityManagerImpl) convertInternalWorkflowExecutionInfo(
	internalExecution *store.InternalWorkflowExecutionInfo,
) (*workflowpb.WorkflowExecutionInfo, error) {
	if internalExecution == nil {
		return nil, nil
	}
	memo, err := deserializeMemo(internalExecution.Memo)
	if err != nil {
		return nil, err
	}

	executionInfo := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: internalExecution.WorkflowID,
			RunId:      internalExecution.RunID,
		},
		Type: &commonpb.WorkflowType{
			Name: internalExecution.TypeName,
		},
		StartTime:        timestamppb.New(internalExecution.StartTime),
		ExecutionTime:    timestamppb.New(internalExecution.ExecutionTime),
		Memo:             memo,
		SearchAttributes: internalExecution.SearchAttributes,
		TaskQueue:        internalExecution.TaskQueue,
		Status:           internalExecution.Status,
		RootExecution: &commonpb.WorkflowExecution{
			WorkflowId: internalExecution.RootWorkflowID,
			RunId:      internalExecution.RootRunID,
		},
		// TODO: poplulate FirstRunId once it has been added as a system search attribute.
	}

	if internalExecution.ParentWorkflowID != "" {
		executionInfo.ParentExecution = &commonpb.WorkflowExecution{
			WorkflowId: internalExecution.ParentWorkflowID,
			RunId:      internalExecution.ParentRunID,
		}
	}

	// for close records
	if internalExecution.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		executionInfo.CloseTime = timestamppb.New(internalExecution.CloseTime)
		executionInfo.ExecutionDuration = durationpb.New(internalExecution.ExecutionDuration)
		executionInfo.HistoryLength = internalExecution.HistoryLength
		executionInfo.HistorySizeBytes = internalExecution.HistorySizeBytes
		executionInfo.StateTransitionCount = internalExecution.StateTransitionCount
	}

	// Workflows created before 1.11 have ExecutionTime set to Unix epoch zero time (1/1/1970) for non-cron/non-retry case.
	// Use StartTime as ExecutionTime for this case (if there was a backoff it must be set).
	// Remove this "if" block when ExecutionTime field has actual correct value (added 6/9/21).
	// Affects only non-advanced visibility.
	if !executionInfo.ExecutionTime.AsTime().After(time.Unix(0, 0)) {
		executionInfo.ExecutionTime = timestamppb.New(internalExecution.StartTime)
	}

	return executionInfo, nil
}

func deserializeMemo(data *commonpb.DataBlob) (*commonpb.Memo, error) {
	if data == nil || len(data.Data) == 0 {
		return &commonpb.Memo{}, nil
	}

	var ()
	switch data.EncodingType {
	case enumspb.ENCODING_TYPE_PROTO3:
		memo := &commonpb.Memo{}
		err := proto.Unmarshal(data.Data, memo)
		if err != nil {
			return nil, serialization.NewDeserializationError(
				enumspb.ENCODING_TYPE_PROTO3, fmt.Errorf("unable to deserialize memo from data blob: %w", err))
		}
		return memo, nil
	default:
		return nil, serialization.NewUnknownEncodingTypeError(data.GetEncodingType().String(), enumspb.ENCODING_TYPE_PROTO3)
	}
}

func serializeMemo(memo *commonpb.Memo) (*commonpb.DataBlob, error) {
	if memo == nil {
		memo = &commonpb.Memo{}
	}

	data, err := proto.Marshal(memo)
	if err != nil {
		return nil, serviceerror.NewInternalf("Unable to serialize memo to data blob: %v", err)
	}

	return &commonpb.DataBlob{
		Data:         data,
		EncodingType: MemoEncoding,
	}, nil
}
