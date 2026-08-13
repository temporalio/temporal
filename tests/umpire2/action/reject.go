package action

// The error / divergence model (UMPIRE_ERR.md): declared variants off an action's valid base, so
// umpire drives invalid inputs and judges the outcome against the same conformance machinery.
// E1 is the rejection round-trip (a rejected RPC is a judged outcome, not a drive crash); E2 adds
// per-field variant enumeration by reflecting the request descriptor. This file holds the
// Temporal concretions; the abstract schema (Reject / Param / Domain / Variant) is in
// common/testing/umpire.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/links"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ---- E1: the rejection round-trip ----

// rpcStartInvalid realizes an invalid StartNexusOperationExecution: a well-formed request naming a
// non-existent endpoint. The frontend rejects it (NotFound) during endpoint resolution, before any
// operation is created, so it returns the RPC error and binds nothing; Drive captures the error as
// the expected rejection (see umpire.Action.Reject).
type rpcStartInvalid struct{}

func (rpcStartInvalid) Install(umpire.RealizeContext, umpire.Action) error { return nil }

func (rpcStartInvalid) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	req := c.validStartBase()
	req.Endpoint = "umpire-nonexistent-endpoint"
	bindFresh(rc, a, req.GetRequestId()) // the rejected op's identity == its request id
	_, err := c.Env.FrontendClient().StartNexusOperationExecution(ctx, req)
	return err
}

// StartUnknownEndpoint is an invalid StartNexusOperationExecution: a well-formed request naming a
// non-existent endpoint. The frontend rejects it (a client error, grounded as NotFound) — the
// rejection is modeled as the op reaching the `rejected` terminal (its reject Effect), so Reconcile
// judges it like any other transition. Reject asserts the generic contract and lets Drive treat the
// RPC error as the expected outcome.
var StartUnknownEndpoint = umpire.Action{
	Name: "StartNexusOperationExecution(unknown-endpoint)", Kind: umpire.ClientRPC, Hosting: umpire.Standalone,
	Effects: []umpire.Effect{{Ref: nexusOp("op", true), Event: model.NexusReject}},
	Reject:  &umpire.Reject{},
	Realize: rpcStartInvalid{},
}

// The rejection is now judged by the model, not a domain-side check: the decoder turns the RPC
// error into a NexusOperationRejected fact (only for client-error classes), the operation reaches
// the `rejected` Failure terminal, and the existing umpire.Reconcile confirms the action's reject
// Effect against the observed edge. The former RejectionDrift / clientErrorCodes helpers are gone;
// the client-error gate lives in fact.RejectionCode (see UMPIRE_ERR.md §3).

// CountEntities reports how many entities of type t the Monitor currently models in the env's
// namespace — used to assert a rejection produced exactly one (rejected) operation.
func CountEntities(env Environment, t umpire.EntityType) int {
	nsRoot := umpire.NewEntityID(model.NamespaceType, env.NamespaceID().String())
	return len(env.GetMonitor().ModelState().QueryEntities(t, 0, &nsRoot))
}

// ---- E2: per-field variant enumeration by descriptor reflection ----

// validStartBase is a known-good StartNexusOperationExecution request (real endpoint, valid
// fields). Variant realizers build from it and perturb exactly one field — the "valid base plus
// one labeled mutation" discipline (UMPIRE_ERR.md §5), so the mutated field is the sole cause of
// any rejection.
func (c *Ctx) validStartBase() *workflowservice.StartNexusOperationExecutionRequest {
	opID := fmt.Sprintf("umpire-action-mut-%d", c.Iter)
	return &workflowservice.StartNexusOperationExecutionRequest{
		Namespace:              c.Env.Namespace().String(),
		OperationId:            opID,
		Endpoint:               c.Endpoint,
		Service:                "service",
		Operation:              "operation",
		RequestId:              opID,
		ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
		Input:                  payloads.MustEncodeSingle("input"),
	}
}

// stringDomain is the reflected domain of a proto string field: its standard invalid neighbors are
// an empty value and an over-long one, both client-error-class (UMPIRE_ERR.md §1).
type stringDomain struct {
	overLen  int
	required bool
}

func (d stringDomain) Variants() []umpire.Variant {
	variants := []umpire.Variant{
		{Label: "too-long", Class: umpire.Malformed, Mutate: func(any) any { return strings.Repeat("x", d.overLen) }, Expect: &umpire.Reject{}},
	}
	if d.required {
		variants = append([]umpire.Variant{{Label: "empty", Class: umpire.Malformed, Mutate: func(any) any { return "" }, Expect: &umpire.Reject{}}}, variants...)
	}
	return variants
}

// durationDomain is the reflected domain of a google.protobuf.Duration field: its standard invalid
// neighbor is a negative value (OutOfRange), which the server rejects before the operation exists.
// It ignores the base value (the mutant is absolute), proving the reflection generalizes past
// strings to message-typed fields (UMPIRE_ERR.md §1, E5).
type durationDomain struct{}

func (durationDomain) Variants() []umpire.Variant {
	return []umpire.Variant{
		{Label: "negative", Class: umpire.OutOfRange, Mutate: func(any) any { return durationpb.New(-time.Second) }, Expect: &umpire.Reject{}},
	}
}

// isDurationField reports whether fd is a scalar google.protobuf.Duration message field.
func isDurationField(fd protoreflect.FieldDescriptor) bool {
	return fd.Kind() == protoreflect.MessageKind && fd.Message().FullName() == "google.protobuf.Duration"
}

// isPayloadField reports whether fd is a scalar Temporal payload field.
func isPayloadField(fd protoreflect.FieldDescriptor) bool {
	return fd.Kind() == protoreflect.MessageKind && fd.Message().FullName() == "temporal.api.common.v1.Payload"
}

func reflectedEnumDomain(fd protoreflect.FieldDescriptor) umpire.Domain {
	values := fd.Enum().Values()
	numbers := make([]int32, values.Len())
	for i := 0; i < values.Len(); i++ {
		numbers[i] = int32(values.Get(i).Number())
	}
	domain, err := umpire.NewEnumDomain(numbers)
	if err != nil {
		return umpire.NewUnsupportedDomain(err.Error())
	}
	return domain
}

func validatorKey(message protoreflect.MessageDescriptor, field protoreflect.FieldDescriptor) string {
	return string(message.FullName()) + "." + string(field.Name())
}

func startValidatorRegistry(msg protoreflect.ProtoMessage) (*umpire.ValidatorRegistry, error) {
	descriptor := msg.ProtoReflect().Descriptor()
	requiredStrings := map[protoreflect.Name]bool{
		"namespace": true, "operation_id": true, "endpoint": true, "service": true, "operation": true,
	}
	var registrations []umpire.ValidatorRegistration
	fields := descriptor.Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if field.IsList() || field.IsMap() {
			continue
		}
		key := validatorKey(descriptor, field)
		switch {
		case field.Kind() == protoreflect.StringKind:
			base := stringDomain{overLen: 4096, required: requiredStrings[field.Name()]}
			registrations = append(registrations, umpire.ValidatorRegistration{
				Key: key, Domain: base,
				Normalize: func(value any) (string, error) {
					text, ok := value.(string)
					if !ok {
						return "", fmt.Errorf("string has type %T", value)
					}
					if base.required && text == "" {
						return "", errors.New("required string is empty")
					}
					if len(text) >= base.overLen {
						return "", fmt.Errorf("string has %d bytes, maximum is %d", len(text), base.overLen-1)
					}
					return text, nil
				},
			})
		case field.Kind() == protoreflect.EnumKind:
			base := reflectedEnumDomain(field)
			normalizing, ok := base.(umpire.NormalizingDomain)
			if !ok {
				return nil, fmt.Errorf("enum validator %s is not normalizing", key)
			}
			registrations = append(registrations, umpire.ValidatorRegistration{Key: key, Domain: base, Normalize: normalizing.Normalize})
		case isDurationField(field):
			registrations = append(registrations, umpire.ValidatorRegistration{
				Key: key, Domain: durationDomain{}, Clone: cloneProtoValue,
				Normalize: func(value any) (string, error) {
					duration, ok := value.(*durationpb.Duration)
					if !ok {
						return "", fmt.Errorf("duration has type %T", value)
					}
					if duration == nil {
						return "duration:zero", nil
					}
					if err := timestamp.ValidateAndCapProtoDuration(duration); err != nil {
						return "", err
					}
					return umpire.CanonicalProtoDigest("duration", duration)
				},
			})
		case isPayloadField(field):
			base := umpire.NewPayloadDomain(2 * 1024 * 1024)
			registrations = append(registrations, umpire.ValidatorRegistration{
				Key: key, Domain: base, Clone: cloneProtoValue,
				Normalize: func(value any) (string, error) {
					input, ok := value.(*commonpb.Payload)
					if !ok {
						return "", fmt.Errorf("payload has type %T", value)
					}
					if input == nil {
						return "payload:nil", nil
					}
					if err := chasmnexus.ValidatePayloadSize(input, 2*1024*1024); err != nil {
						return "", err
					}
					var decoded any
					if err := payload.Decode(input, &decoded); err != nil {
						return "", err
					}
					return base.Normalize(input)
				},
			})
		default:
			continue
		}
	}
	return umpire.NewValidatorRegistry(registrations...)
}

func cloneProtoValue(value any) any {
	message, ok := value.(proto.Message)
	if !ok || message == nil || !message.ProtoReflect().IsValid() {
		return value
	}
	return proto.Clone(message)
}

type linkCollectionDomain struct {
	maxCount int
	maxSize  int
}

func (d linkCollectionDomain) Variants() []umpire.Variant {
	return []umpire.Variant{{
		Label: "too-many", Class: umpire.OutOfRange, Expect: &umpire.Reject{},
		Mutate: func(any) any { return make([]*commonpb.Link, d.maxCount+1) },
	}}
}

func newLinkCollectionValidatorDomain(maxCount, maxSize int) (*umpire.ValidatorDomain, error) {
	registry, err := umpire.NewValidatorRegistry(umpire.ValidatorRegistration{
		Key: "temporal.api.common.v1.Link[]", Domain: linkCollectionDomain{maxCount: maxCount, maxSize: maxSize},
		Clone: func(value any) any {
			values, _ := value.([]*commonpb.Link)
			cloned := slices.Clone(values)
			for index, link := range cloned {
				if link != nil {
					cloned[index] = proto.Clone(link).(*commonpb.Link)
				}
			}
			return cloned
		},
		Normalize: func(value any) (string, error) {
			values, ok := value.([]*commonpb.Link)
			if !ok {
				return "", fmt.Errorf("links have type %T", value)
			}
			if err := links.Validate(values, maxCount, maxSize); err != nil {
				return "", err
			}
			digests := make([]string, 0, len(values))
			for _, link := range values {
				digest, err := umpire.CanonicalProtoDigest("link", link)
				if err != nil {
					return "", err
				}
				digests = append(digests, digest)
			}
			slices.Sort(digests)
			return "links:" + strings.Join(digests, ","), nil
		},
	})
	if err != nil {
		return nil, err
	}
	return registry.Domain("temporal.api.common.v1.Link[]")
}

func newSignedIntegerValidatorDomain(minimum, maximum int64) (*umpire.ValidatorDomain, error) {
	base, err := umpire.NewIntegerDomain(minimum, maximum)
	if err != nil {
		return nil, err
	}
	registry, err := umpire.NewValidatorRegistry(umpire.ValidatorRegistration{
		Key: "signed-integer", Domain: base, Normalize: base.Normalize,
	})
	if err != nil {
		return nil, err
	}
	return registry.Domain("signed-integer")
}

// reflectStartParams walks a request message's descriptor and returns a Param per scalar field the
// reflection understands — string, enum, Duration, and Payload fields. This is the pillar-1
// enumeration (UMPIRE_ERR.md §0): the variant set falls out of the descriptor, not hand
// authoring. Integer domains remain a follow-up until request-specific bounds are declared.
func reflectStartParams(msg protoreflect.ProtoMessage) []umpire.Param {
	registry, registryErr := startValidatorRegistry(msg)
	var params []umpire.Param
	descriptor := msg.ProtoReflect().Descriptor()
	fields := descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if registryErr != nil {
			params = append(params, umpire.Param{Path: string(fd.Name()), Domain: umpire.NewUnsupportedDomain(registryErr.Error())})
			continue
		}
		domain, err := registry.Domain(validatorKey(descriptor, fd))
		if err != nil {
			params = append(params, umpire.Param{Path: string(fd.Name()), Domain: umpire.NewUnsupportedDomain(err.Error())})
			continue
		}
		params = append(params, umpire.Param{Path: string(fd.Name()), Domain: domain})
	}
	return params
}

// rpcStartMutated issues a StartNexusOperationExecution built from the valid base with a single
// field (path) replaced per mutate. Reflection sets the field by its proto name and kind, so the
// same realizer serves every reflected param without a per-field realizer.
type rpcStartMutated struct {
	path   string
	mutate func(valid any) any
}

func (rpcStartMutated) Install(umpire.RealizeContext, umpire.Action) error { return nil }

func (r rpcStartMutated) Fire(ctx context.Context, rc umpire.RealizeContext, a umpire.Action) error {
	c := rc.(*Ctx)
	req := c.validStartBase()
	m := req.ProtoReflect()
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(r.path))
	m.Set(fd, protoValue(fd, r.mutate(currentValue(fd, m))))
	bindFresh(rc, a, req.GetRequestId()) // the rejected op's identity == its (unmutated) request id
	_, err := c.Env.FrontendClient().StartNexusOperationExecution(ctx, req)
	return err
}

// currentValue extracts a field's current value as the Go type its Domain expects.
func currentValue(fd protoreflect.FieldDescriptor, m protoreflect.Message) any {
	switch {
	case fd.Kind() == protoreflect.StringKind:
		return m.Get(fd).String()
	case fd.Kind() == protoreflect.EnumKind:
		return int32(m.Get(fd).Enum())
	case isPayloadField(fd):
		return m.Get(fd).Message().Interface()
	default:
		return nil
	}
}

// protoValue converts a Mutate's result back to a protoreflect.Value for the field's kind.
func protoValue(fd protoreflect.FieldDescriptor, v any) protoreflect.Value {
	switch val := v.(type) {
	case string:
		return protoreflect.ValueOfString(val)
	case int32:
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(val))
	case *durationpb.Duration:
		return protoreflect.ValueOfMessage(val.ProtoReflect())
	case *commonpb.Payload:
		return protoreflect.ValueOfMessage(val.ProtoReflect())
	default:
		panic(fmt.Sprintf("umpire: unsupported mutated value %T for field %s", v, fd.Name()))
	}
}

// StartFieldVariant builds the invalid action for one (field, variant) pair: mutate that field on
// the valid base and expect the variant's outcome. The rejection is modeled as the op
// reaching the `rejected` terminal (the reject Effect), judged by Reconcile; Reject lets Drive
// treat the RPC error as the expected outcome.
func StartFieldVariant(path string, v umpire.Variant) umpire.Action {
	return umpire.Action{
		Name:    fmt.Sprintf("StartNexusOperationExecution(%s=%s)", path, v.Label),
		Kind:    umpire.ClientRPC,
		Hosting: umpire.Standalone,
		Effects: []umpire.Effect{{Ref: nexusOp("op", true), Event: model.NexusReject}},
		Reject:  v.Expect,
		Realize: rpcStartMutated{path: path, mutate: v.Mutate},
	}
}

// StartFieldVariants enumerates the invalid actions for every reflected param × variant of
// StartNexusOperationExecution — the negative-space action set derived from the descriptor. E4
// (the differential validator oracle) will decide which of these actually reject vs. are
// normalized/optional; today the enumeration is proven and specific known-rejecting variants
// round-trip.
func StartFieldVariants() []umpire.Action {
	var actions []umpire.Action
	base := &workflowservice.StartNexusOperationExecutionRequest{
		Namespace: "namespace", Identity: "identity", RequestId: "request", OperationId: "operation-id",
		Endpoint: "endpoint", Service: "service", Operation: "operation",
		ScheduleToCloseTimeout: durationpb.New(time.Minute), Input: payloads.MustEncodeSingle("input"),
	}
	message := base.ProtoReflect()
	for _, p := range reflectStartParams(base) {
		field := message.Descriptor().Fields().ByName(protoreflect.Name(p.Path))
		normalizing, ok := p.Domain.(umpire.NormalizingDomain)
		if !ok || field == nil {
			continue
		}
		for _, v := range p.Domain.Variants() {
			if _, err := normalizing.Normalize(v.Mutate(currentValue(field, message))); err == nil {
				continue
			}
			actions = append(actions, StartFieldVariant(p.Path, v))
		}
	}
	return actions
}
