package tests

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/payload"
)

// TestOperation returns a failurepb.Failure so that it is a proper protobuf type. Its used for
// testing the System Nexus Endpoint, which only accepts protobuf-encoded payloads.
var TestOperation = nexus.NewSyncOperation(
	"TestOperation",
	func(ctx context.Context, input string, options nexus.StartOperationOptions) (*commonpb.DataBlob, error) {
		d := []byte("Hello, " + input)
		return &commonpb.DataBlob{Data: d}, nil
	})

// TestOperationWithPayload is identical to TestOperation, except its response embeds a
// nested *commonpb.Payload. It exists to exercise the commonnexus.SystemPayloadMetadataKey
// flag set in service/history/handler.go's StartNexusOperation.
var TestOperationWithPayload = nexus.NewSyncOperation("TestOperationWithPayload", func(ctx context.Context, input string, options nexus.StartOperationOptions) (*commonpb.Payloads, error) {
	return &commonpb.Payloads{Payloads: []*commonpb.Payload{payload.EncodeString("Hello, " + input)}}, nil
})

// TestOperationStringOutput returns a string, which the data converter encodes as JSON rather
// than protobuf. It exists to exercise the System Nexus Endpoint's rejection of non-protobuf
// responses in service/history/handler.go's StartNexusOperation.
var TestOperationStringOutput = nexus.NewSyncOperation("TestOperationStringOutput", func(ctx context.Context, input string, options nexus.StartOperationOptions) (string, error) {
	return "Hello, " + input, nil
})

func NewTestServiceNexusService() *nexus.Service {
	service := nexus.NewService("TestService")
	service.MustRegister(TestOperation)
	service.MustRegister(TestOperationWithPayload)
	service.MustRegister(TestOperationStringOutput)
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
	sp.MustRegisterOperation("TestOperationStringOutput", chasm.NewRegisterableNexusOperationProcessor(testOperationProcessor{}))
	return sp
}
