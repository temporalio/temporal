//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination visibility_manager_mock.go

package chasm

import (
	"context"
	"reflect"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/common/payload"
	"google.golang.org/protobuf/proto"
)

type VisibilityManager interface {
	ListExecutions(
		context.Context,
		reflect.Type,
		*ListExecutionsRequest,
	) (*visibilityservice.ListChasmExecutionsResponse, error)

	CountExecutions(
		context.Context,
		reflect.Type,
		*CountExecutionsRequest,
	) (*visibilityservice.CountChasmExecutionsResponse, error)
}

type VisibilityExecutionInfo[M proto.Message] struct {
	BusinessID             string
	RunID                  string
	StartTime              time.Time
	CloseTime              time.Time
	HistoryLength          int64
	HistorySizeBytes       int64
	StateTransitionCount   int64
	ChasmSearchAttributes  SearchAttributesMap
	CustomSearchAttributes map[string]*commonpb.Payload
	Memo                   *commonpb.Memo
	ChasmMemo              M
}

type ListExecutionsRequest struct {
	NamespaceName string
	Query         string
	PageSize      int
	NextPageToken []byte
}

type ListExecutionsResponse[M proto.Message] struct {
	Executions    []*VisibilityExecutionInfo[M]
	NextPageToken []byte
}

type CountExecutionsRequest struct {
	NamespaceName string
	Query         string
}

type CountExecutionsResponse struct {
	Count  int64
	Groups []Group
}

type Group struct {
	Values []*commonpb.Payload
	Count  int64
}

// ListExecutions lists the executions of a CHASM archetype given an initial query.
// The query string can specify any combination of CHASM, custom, and predefined/system search attributes.
// The generic parameter C is the CHASM component type used for executions and search attribute filtering.
// The generic parameter M is the type of the memo payload to be unmarshaled from the execution.
// PageSize is required, must be greater than 0.
// NextPageToken is optional, set on subsequent requests to continue listing the next page of executions.
// Note: For CHASM executions, TemporalNamespaceDivision is the predefined search attribute
// that is used to identify the archetype of the execution.
// If the query string does not specify TemporalNamespaceDivision, the archetype C of the request will be used to filter the executions.
// If the initial query already specifies TemporalNamespaceDivision, the archetype C of the request will
// only be used to get the registered SearchAttributes.
func ListExecutions[C Component, M proto.Message](
	ctx context.Context,
	request *ListExecutionsRequest,
) (*ListExecutionsResponse[M], error) {
	archetypeType := reflect.TypeFor[C]()
	response, err := visibilityManagerFromContext(ctx).ListExecutions(ctx, archetypeType, request)
	if err != nil {
		return nil, err
	}

	// Convert response: decode ChasmSearchAttributes and ChasmMemo to type M
	executions := make([]*VisibilityExecutionInfo[M], len(response.Executions))
	for i, execution := range response.Executions {
		chasmSAs, err := newSearchAttributesMapFromProto(execution.ChasmSearchAttributes)
		if err != nil {
			return nil, err
		}

		chasmMemoInterface := reflect.New(reflect.TypeFor[M]().Elem()).Interface()
		chasmMemo, ok := chasmMemoInterface.(M)
		if !ok {
			return nil, serviceerror.NewInternalf("failed to cast chasm memo to type %s", reflect.TypeFor[M]().String())
		}
		if err := payload.Decode(execution.ChasmMemo, chasmMemo); err != nil {
			return nil, serviceerror.NewInternalf("failed to decode chasm memo: %v", err)
		}
		executions[i] = &VisibilityExecutionInfo[M]{
			BusinessID:             execution.BusinessId,
			RunID:                  execution.RunId,
			StartTime:              execution.StartTime.AsTime(),
			CloseTime:              execution.CloseTime.AsTime(),
			HistoryLength:          execution.HistoryLength,
			HistorySizeBytes:       execution.HistorySizeBytes,
			StateTransitionCount:   execution.StateTransitionCount,
			ChasmSearchAttributes:  chasmSAs,
			CustomSearchAttributes: execution.CustomSearchAttributes.GetIndexedFields(),
			Memo:                   execution.Memo,
			ChasmMemo:              chasmMemo,
		}
	}

	return &ListExecutionsResponse[M]{
		Executions:    executions,
		NextPageToken: response.NextPageToken,
	}, nil
}

// CountExecutions counts the executions of a CHASM archetype given an initial query.
// The generic parameter C is the CHASM component type used for executions and search attribute filtering.
// The query string can specify any combination of CHASM, custom, and predefined/system search attributes.
// Note: For CHASM executions, TemporalNamespaceDivision is the predefined search attribute
// that is used to identify the archetype of the execution.
// If the query string does not specify TemporalNamespaceDivision, the archetype C of the request will be used to count the executions.
// If the initial query already specifies TemporalNamespaceDivision, the archetype C of the request will
// only be used to get the registered SearchAttributes.
func CountExecutions[C Component](
	ctx context.Context,
	request *CountExecutionsRequest,
) (*CountExecutionsResponse, error) {
	archetypeType := reflect.TypeFor[C]()
	visResponse, err := visibilityManagerFromContext(ctx).CountExecutions(ctx, archetypeType, request)
	if err != nil {
		return nil, err
	}

	response := &CountExecutionsResponse{
		Count:  visResponse.Count,
		Groups: make([]Group, len(visResponse.Groups)),
	}
	for k, group := range visResponse.Groups {
		response.Groups[k] = Group{
			Values: group.GroupValues,
			Count:  group.Count,
		}
	}
	return response, nil
}

type visibilityManagerCtxKeyType string

const visibilityManagerCtxKey visibilityManagerCtxKeyType = "chasmVisibilityManager"

func NewVisibilityManagerContext(
	ctx context.Context,
	engine VisibilityManager,
) context.Context {
	return context.WithValue(ctx, visibilityManagerCtxKey, engine)
}

func visibilityManagerFromContext(
	ctx context.Context,
) VisibilityManager {
	e, ok := ctx.Value(visibilityManagerCtxKey).(VisibilityManager)
	if !ok {
		return nil
	}
	return e
}
