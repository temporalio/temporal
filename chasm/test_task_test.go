// TODO: move this to chasm_test package
package chasm

import (
	commonpb "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type (
	TestSideEffectTask = commonpb.Payload

	TestDiscardableSideEffectTask struct{}

	TestOutboundSideEffectTask struct{}

	TestPureTask commonpb.Payload

	// Singleton task types — distinct Go types so the registry can tell them apart.
	TestSingletonReplaceSideEffectTask commonpb.Payload
	TestSingletonIgnoreSideEffectTask  commonpb.Payload
	TestSingletonReplacePureTask       commonpb.Payload
	TestSingletonIgnorePureTask        commonpb.Payload
)

// ProtoReflect implements [protoreflect.ProtoMessage].
func (t *TestPureTask) ProtoReflect() protoreflect.Message {
	return (*commonpb.Payload)(t).ProtoReflect()
}

func (t *TestSingletonReplaceSideEffectTask) ProtoReflect() protoreflect.Message {
	return (*commonpb.Payload)(t).ProtoReflect()
}

func (t *TestSingletonIgnoreSideEffectTask) ProtoReflect() protoreflect.Message {
	return (*commonpb.Payload)(t).ProtoReflect()
}

func (t *TestSingletonReplacePureTask) ProtoReflect() protoreflect.Message {
	return (*commonpb.Payload)(t).ProtoReflect()
}

func (t *TestSingletonIgnorePureTask) ProtoReflect() protoreflect.Message {
	return (*commonpb.Payload)(t).ProtoReflect()
}

var (
	_ proto.Message = (*TestPureTask)(nil)
	_ proto.Message = (*TestSingletonReplaceSideEffectTask)(nil)
	_ proto.Message = (*TestSingletonIgnoreSideEffectTask)(nil)
	_ proto.Message = (*TestSingletonReplacePureTask)(nil)
	_ proto.Message = (*TestSingletonIgnorePureTask)(nil)
)
