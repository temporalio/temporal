package nexusoperation

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

var endpointEntry = &persistencespb.NexusEndpointEntry{
	Id: "endpoint-id",
	Endpoint: &persistencespb.NexusEndpoint{
		Spec: &persistencespb.NexusEndpointSpec{
			Name: "endpoint",
			Target: &persistencespb.NexusEndpointTarget{
				Variant: &persistencespb.NexusEndpointTarget_External_{
					External: &persistencespb.NexusEndpointTarget_External{
						Url: "http://" + uuid.NewString(),
					},
				},
			},
		},
	},
}

func mustToPayload(t *testing.T, input any) *commonpb.Payload {
	conv := converter.GetDefaultDataConverter()
	payload, err := conv.ToPayload(input)
	require.NoError(t, err)
	return payload
}

// mockStoreComponent is a mock parent component that implements OperationStore.
// It allows the Operation to load its start args and apply transitions.
// TODO(stephan): Remove this layer from tests once loading invocation data from the operation component is implemented.
type mockStoreComponent struct {
	chasm.UnimplementedComponent

	// Data is required by CHASM for serialization - every component needs a proto.Message field.
	Data *nexusoperationpb.OperationState

	invocationData  InvocationData
	Op              chasm.Field[*Operation]
	startLinks      []*commonpb.Link
	completionLinks []*commonpb.Link
	startTime       *time.Time
}

func (m *mockStoreComponent) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (m *mockStoreComponent) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

func (m *mockStoreComponent) Terminate(_ chasm.MutableContext, _ chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, nil
}

func (m *mockStoreComponent) NexusOperationInvocationData(_ chasm.Context, _ *Operation) (InvocationData, error) {
	return m.invocationData, nil
}

func (m *mockStoreComponent) OnNexusOperationStarted(ctx chasm.MutableContext, op *Operation, operationToken string, startTime *time.Time, links []*commonpb.Link) error {
	m.startTime = startTime
	m.startLinks = links
	return TransitionStarted.Apply(op, ctx, EventStarted{
		OperationToken: operationToken,
		StartTime:      startTime,
	})
}

func (m *mockStoreComponent) OnNexusOperationCompleted(ctx chasm.MutableContext, op *Operation, result *commonpb.Payload, links []*commonpb.Link) error {
	m.completionLinks = links
	return TransitionSucceeded.Apply(op, ctx, EventSucceeded{Result: result})
}

func (m *mockStoreComponent) OnNexusOperationFailed(ctx chasm.MutableContext, op *Operation, cause *failurepb.Failure) error {
	return TransitionFailed.Apply(op, ctx, EventFailed{Failure: cause})
}

func (m *mockStoreComponent) OnNexusOperationCanceled(ctx chasm.MutableContext, op *Operation, cause *failurepb.Failure) error {
	return TransitionCanceled.Apply(op, ctx, EventCanceled{Failure: cause})
}

func (m *mockStoreComponent) OnNexusOperationTimedOut(ctx chasm.MutableContext, op *Operation, cause *failurepb.Failure, fromAttempt bool) error {
	return TransitionTimedOut.Apply(op, ctx, EventTimedOut{
		Failure:     cause,
		FromAttempt: fromAttempt,
	})
}

func (m *mockStoreComponent) OnNexusOperationCancellationCompleted(ctx chasm.MutableContext, op *Operation) error {
	cancellation, _ := op.Cancellation.TryGet(ctx)
	return TransitionCancellationSucceeded.Apply(cancellation, ctx, EventCancellationSucceeded{})
}

func (m *mockStoreComponent) OnNexusOperationCancellationFailed(ctx chasm.MutableContext, op *Operation, cause *failurepb.Failure) error {
	cancellation, _ := op.Cancellation.TryGet(ctx)
	return TransitionCancellationFailed.Apply(cancellation, ctx, EventCancellationFailed{Failure: cause})
}

// mockStoreLibrary registers the mockStoreComponent so the CHASM tree can work with it.
type mockStoreLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *mockStoreLibrary) Name() string {
	return "mockStore"
}

func (l *mockStoreLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*mockStoreComponent]("mockStore"),
	}
}
