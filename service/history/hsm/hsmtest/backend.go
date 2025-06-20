package hsmtest

import (
	"context"
	"fmt"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
)

type NodeBackend struct {
	Events []*historypb.HistoryEvent
}

func (n *NodeBackend) GetCurrentVersion() int64 {
	return 1
}

func (n *NodeBackend) NextTransitionCount() int64 {
	return 3
}

func (n *NodeBackend) AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
	event := &historypb.HistoryEvent{EventType: t, EventId: 2}
	setAttributes(event)
	n.Events = append(n.Events, event)
	return event
}

func (n *NodeBackend) GenerateEventLoadToken(event *historypb.HistoryEvent) ([]byte, error) {
	token := &tokenspb.HistoryEventRef{
		EventId:      event.EventId,
		EventBatchId: event.EventId,
	}
	return proto.Marshal(token)
}

func (n *NodeBackend) LoadHistoryEvent(ctx context.Context, tokenBytes []byte) (*historypb.HistoryEvent, error) {
	var token tokenspb.HistoryEventRef
	if err := proto.Unmarshal(tokenBytes, &token); err != nil {
		return nil, err
	}
	idx := slices.IndexFunc(n.Events, func(event *historypb.HistoryEvent) bool {
		return event.EventId == token.EventId
	})

	if idx < 0 {
		return nil, fmt.Errorf("event not found")
	}

	return n.Events[idx], nil
}

var _ hsm.NodeBackend = &NodeBackend{}
