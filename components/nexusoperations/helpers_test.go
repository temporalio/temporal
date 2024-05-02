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

// Helper methods for the nexusoperations_test package.
package nexusoperations_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeEnv struct {
	node *hsm.Node
}

func (s fakeEnv) Access(ctx context.Context, ref hsm.Ref, accessType hsm.AccessType, accessor func(*hsm.Node) error) error {
	return accessor(s.node)
}

func (fakeEnv) Now() time.Time {
	return time.Now()
}

var _ hsm.Environment = fakeEnv{}

func newRegistry(t *testing.T) *hsm.Registry {
	t.Helper()
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, nexusoperations.RegisterStateMachines(reg))
	require.NoError(t, nexusoperations.RegisterEventDefinitions(reg))
	return reg
}

func newRoot(t *testing.T, backend *nodeBackend) *hsm.Node {
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, nexusoperations.RegisterStateMachines(reg))
	root, err := hsm.NewRoot(reg, workflow.StateMachineType.ID, root{}, make(map[int32]*persistence.StateMachineMap), backend)
	require.NoError(t, err)
	return root
}

func newOperationNode(t *testing.T, backend *nodeBackend, schedTime time.Time, timeout time.Duration) *hsm.Node {
	root := newRoot(t, backend)
	event := &historypb.HistoryEvent{
		EventId:   1,
		EventTime: timestamppb.New(schedTime),
		Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
				Endpoint:               "endpoint",
				Service:                "service",
				Operation:              "operation",
				ScheduleToCloseTimeout: durationpb.New(timeout),
			},
		},
	}
	token, err := hsm.GenerateEventLoadToken(event)
	require.NoError(t, err)
	node, err := nexusoperations.AddChild(root, "test-id", event, token, false)
	require.NoError(t, err)
	return node
}

type root struct{}

func (root) IsWorkflowExecutionRunning() bool {
	return true
}

type nodeBackend struct {
	events []*historypb.HistoryEvent
}

func (n *nodeBackend) AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
	event := &historypb.HistoryEvent{EventType: t, EventId: 2}
	setAttributes(event)
	n.events = append(n.events, event)
	return event
}

func (n *nodeBackend) GenerateEventLoadToken(event *historypb.HistoryEvent) ([]byte, error) {
	token := &tokenspb.HistoryEventRef{
		EventId:      event.EventId,
		EventBatchId: event.EventId,
	}
	return proto.Marshal(token)
}

func (n *nodeBackend) LoadHistoryEvent(ctx context.Context, token []byte) (*historypb.HistoryEvent, error) {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload("input")
	if err != nil {
		return nil, err
	}

	event := &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		EventId:   2,
		Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
				Service:   "service",
				Operation: "operation",
				Input:     payload,
				RequestId: uuid.NewString(),
			},
		},
	}
	return event, nil
}
