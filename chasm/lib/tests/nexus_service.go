package tests

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/payload"
)

var TestOperation = nexus.NewSyncOperation("TestOperation", func(ctx context.Context, input string, options nexus.StartOperationOptions) (string, error) {
	return "Hello, " + input, nil
})

// TestOperationWithPayload is identical to TestOperation, except its response embeds a
// nested *commonpb.Payload. It exists to exercise the system-nexus-endpoint payload
// metadata flag set in service/history/handler.go's StartNexusOperation.
var TestOperationWithPayload = nexus.NewSyncOperation("TestOperationWithPayload", func(ctx context.Context, input string, options nexus.StartOperationOptions) (*commonpb.Payloads, error) {
	return &commonpb.Payloads{Payloads: []*commonpb.Payload{payload.EncodeString("Hello, " + input)}}, nil
})

func NewTestServiceNexusService() *nexus.Service {
	service := nexus.NewService("TestService")
	service.MustRegister(TestOperation)
	service.MustRegister(TestOperationWithPayload)
	return service
}

type testOperationProcessor struct {
}

func (o testOperationProcessor) ProcessInput(ctx chasm.NexusOperationProcessorContext, input string) (*chasm.NexusOperationProcessorResult, error) {
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  input,
		},
	}, nil
}

func NewTestServiceNexusServiceProcessor() *chasm.NexusServiceProcessor {
	sp := chasm.NewNexusServiceProcessor("TestService")
	sp.MustRegisterOperation("TestOperation", chasm.NewRegisterableNexusOperationProcessor(testOperationProcessor{}))
	sp.MustRegisterOperation("TestOperationWithPayload", chasm.NewRegisterableNexusOperationProcessor(testOperationProcessor{}))
	return sp
}
