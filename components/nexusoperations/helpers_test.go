// Helper methods for the nexusoperations_test package.
package nexusoperations_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/workflow"
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

func newRoot(t *testing.T, backend *hsmtest.NodeBackend) *hsm.Node {
	reg := hsm.NewRegistry()
	require.NoError(t, workflow.RegisterStateMachine(reg))
	require.NoError(t, nexusoperations.RegisterStateMachines(reg))
	root, err := hsm.NewRoot(reg, workflow.StateMachineType, root{}, make(map[string]*persistencespb.StateMachineMap), backend)
	require.NoError(t, err)
	return root
}

func newOperationNode(t *testing.T, backend *hsmtest.NodeBackend, event *historypb.HistoryEvent) *hsm.Node {
	root := newRoot(t, backend)
	token, err := hsm.GenerateEventLoadToken(event)
	require.NoError(t, err)
	node, err := nexusoperations.AddChild(root, fmt.Sprintf("%d", event.EventId), event, token)
	require.NoError(t, err)
	return node
}

type root struct{}

func (root) IsWorkflowExecutionRunning() bool {
	return true
}

func (root) IsTransitionHistoryEnabled() bool {
	return false
}

func mustNewScheduledEvent(schedTime time.Time, timeout time.Duration) *historypb.HistoryEvent {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload("input")
	if err != nil {
		panic(err)
	}

	attr := &historypb.NexusOperationScheduledEventAttributes{
		EndpointId: "endpoint-id",
		Endpoint:   "endpoint",
		Service:    "service",
		Operation:  "operation",
		Input:      payload,
		RequestId:  uuid.NewString(),
	}
	if timeout > 0 {
		attr.ScheduleToCloseTimeout = durationpb.New(timeout)
	}

	return &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
		EventId:   1,
		EventTime: timestamppb.New(schedTime),
		Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
			NexusOperationScheduledEventAttributes: attr,
		},
	}
}
