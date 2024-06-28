// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package hsmtest

import (
	"context"
	"fmt"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"

	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/service/history/hsm"
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
		return nil, fmt.Errorf("event not found") // nolint:goerr113
	}

	return n.Events[idx], nil
}

var _ hsm.NodeBackend = &NodeBackend{}
