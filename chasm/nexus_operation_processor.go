package chasm

import (
	"fmt"
	"math/rand/v2"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
)

// NexusOperationProcessorContext contains context for processing a Nexus operation's input, including the target
// namespace and a request identifier used for tracing and idempotency.
type NexusOperationProcessorContext struct {
	// Namespace is the target namespace used for routing and validation of Nexus operations.
	Namespace *namespace.Namespace
	// RequestID is a unique identifier for the incoming operation request, used for correlation and idempotency
	// across retries.
	RequestID string
	Links     []nexus.Link
	// A boolean indicating whether the operation processor framework should re-serialize the input and store it in the
	// [NexusOperationProcessorResult.ReserializedInputPayload] field. Processor implementations may mutate the input regardless.
	ReserializeInputPayload bool
}

// NexusOperationRoutingKey determines which history shard should process a Nexus operation.
// Different implementations provide different routing strategies (e.g., by execution, random).
type NexusOperationRoutingKey interface {
	// ShardID returns the target shard ID for this routing key given the total number of shards.
	ShardID(numShards int32) int32
}

// NexusOperationRoutingKeyExecution routes operations to a specific shard based on an execution key.
// This ensures that operations related to the same execution are processed on the same shard.
type NexusOperationRoutingKeyExecution struct {
	// NamespaceID is the ID of the namespace containing the execution whose shard should be targeted.
	NamespaceID string
	// BusinessID is the business identifier (e.g., workflow ID) of the execution.
	BusinessID string
}

// ShardID returns the shard that owns the execution identified by the namespace and business IDs.
func (r NexusOperationRoutingKeyExecution) ShardID(numShards int32) int32 {
	return common.WorkflowIDToHistoryShard(r.NamespaceID, r.BusinessID, numShards)
}

// NexusOperationRoutingKeyRandom routes operations to a random shard.
// The ShardID method returns a randomly selected valid shard ID.
type NexusOperationRoutingKeyRandom struct {
}

// ShardID returns a randomly selected shard ID in the range [1, numShards].
func (NexusOperationRoutingKeyRandom) ShardID(numShards int32) int32 {
	return rand.Int32N(numShards) + 1
}

// NexusOperationProcessorResult contains the result of processing a Nexus operation input,
// including the routing key that determines which shard should handle the operation.
type NexusOperationProcessorResult struct {
	// RoutingKey determines which history shard should process the operation.
	RoutingKey NexusOperationRoutingKey
	// A field set by the framework to containing the re-serialized input payload if requested in the given context.
	ReserializedInputPayload *commonpb.Payload
}

// NexusOperationProcessor is an interface that can be implemented per operation to validate and determine routing for the operation.
type NexusOperationProcessor[I any] interface {
	// ProcessInput validates the input, and returns routing information for processing this operation. The
	// method may mutate the input to set default values.
	ProcessInput(ctx NexusOperationProcessorContext, input I) (*NexusOperationProcessorResult, error)
}

// RegisterableNexusOperationProcessor adapts a typed Nexus operation processor for dynamic registration
// and invocation within a service processor.
type RegisterableNexusOperationProcessor struct {
	processInput func(ctx NexusOperationProcessorContext, input *commonpb.Payload) (*NexusOperationProcessorResult, error)
}

func nexusOperationProcessorAdapter[I any](processor NexusOperationProcessor[I]) func(ctx NexusOperationProcessorContext, input *commonpb.Payload) (*NexusOperationProcessorResult, error) {
	return func(ctx NexusOperationProcessorContext, input *commonpb.Payload) (*NexusOperationProcessorResult, error) {
		var i I
		if err := payloads.Decode(&commonpb.Payloads{Payloads: []*commonpb.Payload{input}}, &i); err != nil {
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to decode input payload: %v", err)
		}
		result, err := processor.ProcessInput(ctx, i)
		if err != nil {
			return nil, err
		}
		if ctx.ReserializeInputPayload {
			pls, err := payloads.Encode(i)
			if err != nil {
				herr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "failed to re-encode input payload: %v", err)
				herr.RetryBehavior = nexus.HandlerErrorRetryBehaviorNonRetryable
				return nil, herr
			}
			if len(pls.Payloads) == 1 {
				result.ReserializedInputPayload = pls.Payloads[0]
			}
		}
		return result, nil
	}
}

// NewRegisterableNexusOperationProcessor wraps a typed NexusOperationProcessor and returns a registerable adapter.
func NewRegisterableNexusOperationProcessor[I any](op NexusOperationProcessor[I]) RegisterableNexusOperationProcessor {
	return RegisterableNexusOperationProcessor{
		processInput: nexusOperationProcessorAdapter(op),
	}
}

// NexusServiceProcessor handles input processing for operations within a specific Nexus service.
// It uses reflection to dynamically invoke the ProcessInput method on operations that implement it.
type NexusServiceProcessor struct {
	name       string
	operations map[string]RegisterableNexusOperationProcessor
}

// NewNexusServiceProcessor constructs a processor for a single Nexus service that can register and invoke operation
// processors by name.
func NewNexusServiceProcessor(name string) *NexusServiceProcessor {
	return &NexusServiceProcessor{
		name:       name,
		operations: make(map[string]RegisterableNexusOperationProcessor),
	}
}

// RegisterOperation registers a named operation with this service processor.
// Returns an error if an operation with the same name is already registered.
func (p *NexusServiceProcessor) RegisterOperation(name string, op RegisterableNexusOperationProcessor) error {
	if _, exists := p.operations[name]; exists {
		return fmt.Errorf("operation %q already registered", name)
	}
	p.operations[name] = op
	return nil
}

// MustRegisterOperation registers a named operation and panics if registration fails.
func (p *NexusServiceProcessor) MustRegisterOperation(name string, op RegisterableNexusOperationProcessor) {
	if err := p.RegisterOperation(name, op); err != nil {
		// nolint:forbidigo // Panic is acceptable here for Must-style method.
		panic(err)
	}
}

// ProcessInput routes the input processing request to the appropriate operation processor and returns routing information
// for the operation.
//
// Returns a nexus.HandlerError if the operation is not found or if input processing fails.
func (p *NexusServiceProcessor) ProcessInput(ctx NexusOperationProcessorContext, opName string, input *commonpb.Payload) (*NexusOperationProcessorResult, error) {
	op, ok := p.operations[opName]
	if !ok {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "operation %q not found", opName)
	}

	return op.processInput(ctx, input)
}

// NexusEndpointProcessor handles input processing for Nexus operations across multiple services within a Nexus
// endpoint. It routes requests to the appropriate service processor based on the service name.
type NexusEndpointProcessor struct {
	serviceProcessors map[string]*NexusServiceProcessor
}

// NewNexusEndpointProcessor creates a new NexusEndpointProcessor.
func NewNexusEndpointProcessor() *NexusEndpointProcessor {
	return &NexusEndpointProcessor{
		serviceProcessors: make(map[string]*NexusServiceProcessor),
	}
}

// RegisterServiceProcessor adds a service-level processor to the endpoint keyed by its name.
// Returns an error if a processor with the same name is already registered.
func (p *NexusEndpointProcessor) RegisterServiceProcessor(processor *NexusServiceProcessor) error {
	if _, exists := p.serviceProcessors[processor.name]; exists {
		return fmt.Errorf("service processor %q already registered", processor.name)
	}
	p.serviceProcessors[processor.name] = processor
	return nil
}

// MustRegisterServiceProcessor registers the service processor and panics if registration fails.
func (p *NexusEndpointProcessor) MustRegisterServiceProcessor(processor *NexusServiceProcessor) {
	if err := p.RegisterServiceProcessor(processor); err != nil {
		// nolint:forbidigo // Panic is acceptable here for Must-style method.
		panic(err)
	}
}

// ProcessInput routes the input processing request to the appropriate service processor and returns routing information
// for the operation.
//
// Returns a nexus.HandlerError if the service is not found or if input processing fails.
func (p *NexusEndpointProcessor) ProcessInput(ctx NexusOperationProcessorContext, service, operation string, input *commonpb.Payload) (*NexusOperationProcessorResult, error) {
	serviceProcessor, ok := p.serviceProcessors[service]
	if !ok {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "service %q not found", service)
	}
	return serviceProcessor.ProcessInput(ctx, operation, input)
}
