package chasm

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
)

type processableInput struct {
	Value int
}

type processableOperation struct {
}

func (o *processableOperation) Name() string {
	return "processable-operation"
}

func (o *processableOperation) ProcessInput(ctx NexusOperationProcessorContext, input *processableInput) (*NexusOperationProcessorResult, error) {
	if input.Value < 0 {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "value must be non-negative")
	}
	// Mutate to test overwrite behavior.
	input.Value += 1
	return &NexusOperationProcessorResult{
		RoutingKey: NexusOperationRoutingKeyRandom{},
	}, nil
}

func newTestContext() NexusOperationProcessorContext {
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:   "test-namespace-id",
			Name: "test-namespace",
		},
		&persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(1),
		},
		"active-cluster",
	)
	return NexusOperationProcessorContext{
		Namespace: ns,
		RequestID: "test-request-id",
	}
}

func mustToPayload(t *testing.T, value any) *commonpb.Payload {
	t.Helper()
	ps, err := payloads.Encode(value)
	require.NoError(t, err)
	return ps.Payloads[0]
}

func TestNexusOperationProcessor_ProcessInput(t *testing.T) {
	t.Parallel()

	processableOp := &processableOperation{}

	processor := NewRegisterableNexusOperationProcessor(processableOp)

	tests := []struct {
		name        string
		ctx         NexusOperationProcessorContext
		input       *commonpb.Payload
		checkResult func(*testing.T, *NexusOperationProcessorResult, error)
	}{
		{
			name:  "valid input returns routing key, no overwrite",
			ctx:   newTestContext(),
			input: mustToPayload(t, processableInput{Value: 23}),
			checkResult: func(t *testing.T, result *NexusOperationProcessorResult, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.NotNil(t, result.RoutingKey)
				require.Nil(t, result.ReserializedInputPayload)
			},
		},
		{
			name: "overwrite payload with valid input",
			ctx: func() NexusOperationProcessorContext {
				ctx := newTestContext()
				ctx.ReserializeInputPayload = true
				return ctx
			}(),
			input: mustToPayload(t, processableInput{Value: 23}),
			checkResult: func(t *testing.T, result *NexusOperationProcessorResult, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.NotNil(t, result.RoutingKey)
				mutatedInput := processableInput{}
				require.NoError(t, payloads.Decode(&commonpb.Payloads{Payloads: []*commonpb.Payload{result.ReserializedInputPayload}}, &mutatedInput))
				require.Equal(t, 24, mutatedInput.Value)
			},
		},
		{
			name:  "invalid input",
			ctx:   newTestContext(),
			input: mustToPayload(t, processableInput{Value: -1}),
			checkResult: func(t *testing.T, nopr *NexusOperationProcessorResult, err error) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
				require.Contains(t, handlerErr.Error(), "value must be non-negative")
			},
		},
		{
			name:  "decode error",
			ctx:   newTestContext(),
			input: mustToPayload(t, "wrong type"),
			checkResult: func(t *testing.T, nopr *NexusOperationProcessorResult, err error) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
				require.Contains(t, handlerErr.Error(), "failed to decode input payload")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := processor.processInput(tt.ctx, tt.input)
			tt.checkResult(t, result, err)
		})
	}
}

func TestNexusServiceProcessor_ProcessInput(t *testing.T) {
	t.Parallel()

	processableOp := &processableOperation{}

	processor := NewNexusServiceProcessor("test-service")
	processor.MustRegisterOperation(processableOp.Name(), NewRegisterableNexusOperationProcessor(processableOp))

	ctx := newTestContext()

	tests := []struct {
		name        string
		opName      string
		input       *commonpb.Payload
		checkResult func(*testing.T, *NexusOperationProcessorResult, error)
	}{
		{
			name:   "operation not found",
			opName: "nonexistent-operation",
			input:  mustToPayload(t, processableInput{Value: 50}),
			checkResult: func(t *testing.T, result *NexusOperationProcessorResult, err error) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeNotFound, handlerErr.Type)
				require.Contains(t, handlerErr.Error(), `operation "nonexistent-operation" not found`)
			},
		},
		{
			name:   "valid input",
			opName: "processable-operation",
			input:  mustToPayload(t, processableInput{Value: 50}),
			checkResult: func(t *testing.T, result *NexusOperationProcessorResult, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.NotNil(t, result.RoutingKey)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := processor.ProcessInput(ctx, tt.opName, tt.input)
			tt.checkResult(t, result, err)
		})
	}
}

func TestNexusEndpointProcessor_ProcessInput(t *testing.T) {
	t.Parallel()

	processableOp := &processableOperation{}

	serviceProcessor := NewNexusServiceProcessor("test-service")
	serviceProcessor.MustRegisterOperation(processableOp.Name(), NewRegisterableNexusOperationProcessor(processableOp))

	processor := NewNexusEndpointProcessor()
	processor.MustRegisterServiceProcessor(serviceProcessor)

	ctx := newTestContext()

	tests := []struct {
		name        string
		service     string
		operation   string
		input       *commonpb.Payload
		checkResult func(*testing.T, *NexusOperationProcessorResult, error)
	}{
		{
			name:      "service not found",
			service:   "nonexistent-service",
			operation: "processable-operation",
			input:     mustToPayload(t, processableInput{Value: 50}),
			checkResult: func(t *testing.T, result *NexusOperationProcessorResult, err error) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, err, &handlerErr)
				require.Equal(t, nexus.HandlerErrorTypeNotFound, handlerErr.Type)
				require.Contains(t, handlerErr.Error(), `service "nonexistent-service" not found`)
			},
		},
		{
			name:      "valid request",
			service:   "test-service",
			operation: "processable-operation",
			input:     mustToPayload(t, processableInput{Value: 47}),
			checkResult: func(t *testing.T, result *NexusOperationProcessorResult, err error) {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.NotNil(t, result.RoutingKey)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := processor.ProcessInput(ctx, tt.service, tt.operation, tt.input)
			tt.checkResult(t, result, err)
		})
	}
}
