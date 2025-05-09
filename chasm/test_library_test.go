// TODO: move this to chasm_test package
package chasm

import (
	"go.uber.org/mock/gomock"
)

type TestLibrary struct {
	UnimplementedLibrary

	controller *gomock.Controller
}

func newTestLibrary(
	controller *gomock.Controller,
) *TestLibrary {
	return &TestLibrary{
		controller: controller,
	}
}

func (l *TestLibrary) Name() string {
	return "TestLibrary"
}

func (l *TestLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{
		NewRegistrableComponent[*CollectionComponent]("collection_component"),
		NewRegistrableComponent[*TestComponent]("test_component"),
		NewRegistrableComponent[*TestSubComponent1]("test_sub_component_1"),
		NewRegistrableComponent[*TestSubComponent11]("test_sub_component_11"),
		NewRegistrableComponent[*TestSubComponent2]("test_sub_component_2"),
	}
}

func (l *TestLibrary) Tasks() []*RegistrableTask {
	return []*RegistrableTask{
		NewRegistrableSideEffectTask(
			"test_side_effect_task",
			NewMockTaskValidator[any, *TestSideEffectTask](l.controller),
			NewMockSideEffectTaskExecutor[any, *TestSideEffectTask](l.controller),
		),
		NewRegistrableSideEffectTask(
			// NOTE this task is registered as a struct, instead of pointer to struct.
			"test_outbound_side_effect_task",
			NewMockTaskValidator[any, TestOutboundSideEffectTask](l.controller),
			NewMockSideEffectTaskExecutor[any, TestOutboundSideEffectTask](l.controller),
		),
		NewRegistrablePureTask(
			"test_pure_task",
			NewMockTaskValidator[any, *TestPureTask](l.controller),
			NewMockPureTaskExecutor[any, *TestPureTask](l.controller),
		),
	}
}
