// TODO: move this to chasm_test package
package chasm

import (
	commonpb "go.temporal.io/api/common/v1"
)

type (
	TestSideEffectTask = commonpb.Payload

	TestDiscardableSideEffectTask struct{}

	TestOutboundSideEffectTask struct{}

	TestPureTask struct {
		Payload *commonpb.Payload
	}
)
