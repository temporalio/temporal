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
)

// ProtoReflect implements [protoreflect.ProtoMessage].
func (t *TestPureTask) ProtoReflect() protoreflect.Message {
	return (*commonpb.Payload)(t).ProtoReflect()
}

var _ proto.Message = (*TestPureTask)(nil)
