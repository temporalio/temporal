package tests

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
)

var TestOperation = nexus.NewSyncOperation("TestOperation", func(ctx context.Context, input string, options nexus.StartOperationOptions) (string, error) {
	return "Hello, " + input, nil
})

func NewTestServiceNexusService() *nexus.Service {
	service := nexus.NewService("TestService")
	service.MustRegister(TestOperation)
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
	return sp
}
