package action

// The error / divergence model (UMPIRE_ERR.md): declared variants off an action's valid base, so
// umpire drives invalid inputs and judges the outcome against the same conformance machinery.
// E1 is the rejection round-trip (a rejected RPC is a judged outcome, not a drive crash); E2 adds
// per-field variant enumeration by reflecting the request descriptor. This file holds the
// Temporal concretions; the abstract schema (Reject / Param / Domain / Variant) is in
// common/testing/umpire.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
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
	}
}

// stringDomain is the reflected domain of a proto string field: its standard invalid neighbors are
// an empty value and an over-long one, both client-error-class (UMPIRE_ERR.md §1).
type stringDomain struct{ overLen int }

func (d stringDomain) Variants() []umpire.Variant {
	return []umpire.Variant{
		{Label: "empty", Class: umpire.Malformed, Mutate: func(any) any { return "" }, Expect: &umpire.Reject{}},
		{Label: "too-long", Class: umpire.Malformed, Mutate: func(any) any { return strings.Repeat("x", d.overLen) }, Expect: &umpire.Reject{}},
	}
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

// reflectStartParams walks a request message's descriptor and returns a Param per scalar field the
// reflection understands — string fields (stringDomain) and Duration fields (durationDomain). This
// is the pillar-1 enumeration (UMPIRE_ERR.md §0): the variant set falls out of the descriptor, not
// hand authoring. Enum / int / payload domains are further follow-ups.
func reflectStartParams(msg protoreflect.ProtoMessage) []umpire.Param {
	var params []umpire.Param
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.IsList() || fd.IsMap() {
			continue
		}
		switch {
		case fd.Kind() == protoreflect.StringKind:
			params = append(params, umpire.Param{Path: string(fd.Name()), Domain: stringDomain{overLen: 4096}})
		case isDurationField(fd):
			params = append(params, umpire.Param{Path: string(fd.Name()), Domain: durationDomain{}})
		}
	}
	return params
}

// rpcStartMutated issues a StartNexusOperationExecution built from the valid base with a single
// field (path) replaced per mutate. Reflection sets the field by its proto name and kind, so the
// same realizer serves every reflected param (string or Duration) without a per-field realizer.
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

// currentValue extracts a field's current value as the Go type a Mutate expects (a string for
// string fields; nil for Duration fields, whose mutants are absolute).
func currentValue(fd protoreflect.FieldDescriptor, m protoreflect.Message) any {
	if fd.Kind() == protoreflect.StringKind {
		return m.Get(fd).String()
	}
	return nil
}

// protoValue converts a Mutate's result back to a protoreflect.Value for the field's kind.
func protoValue(fd protoreflect.FieldDescriptor, v any) protoreflect.Value {
	switch val := v.(type) {
	case string:
		return protoreflect.ValueOfString(val)
	case *durationpb.Duration:
		return protoreflect.ValueOfMessage(val.ProtoReflect())
	default:
		panic(fmt.Sprintf("umpire: unsupported mutated value %T for field %s", v, fd.Name()))
	}
}

// StartFieldVariant builds the invalid action for one (string field, variant) pair: mutate that
// field on the valid base and expect the variant's outcome. The rejection is modeled as the op
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
// StartNexusOperationExecution — the negative-space action set derived from the descriptor (string
// and Duration fields). E4 (the differential validator oracle) will decide which of these actually
// reject vs. are normalized/optional; today the enumeration is proven and specific known-rejecting
// variants round-trip.
func StartFieldVariants() []umpire.Action {
	var actions []umpire.Action
	for _, p := range reflectStartParams(&workflowservice.StartNexusOperationExecutionRequest{}) {
		for _, v := range p.Domain.Variants() {
			actions = append(actions, StartFieldVariant(p.Path, v))
		}
	}
	return actions
}
