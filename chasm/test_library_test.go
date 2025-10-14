// TODO: move this to chasm_test package
package chasm

import (
	"go.uber.org/mock/gomock"
)

type TestLibrary struct {
	UnimplementedLibrary

	controller *gomock.Controller

	mockSideEffectTaskValidator         *MockTaskValidator[any, *TestSideEffectTask]
	mockSideEffectTaskExecutor          *MockSideEffectTaskExecutor[any, *TestSideEffectTask]
	mockOutboundSideEffectTaskValidator *MockTaskValidator[any, TestOutboundSideEffectTask]
	mockOutboundSideEffectTaskExecutor  *MockSideEffectTaskExecutor[any, TestOutboundSideEffectTask]
	mockPureTaskValidator               *MockTaskValidator[any, *TestPureTask]
	mockPureTaskExecutor                *MockPureTaskExecutor[any, *TestPureTask]
}

func newTestLibrary(
	controller *gomock.Controller,
) *TestLibrary {
	return &TestLibrary{
		controller: controller,

		mockSideEffectTaskValidator:         NewMockTaskValidator[any, *TestSideEffectTask](controller),
		mockSideEffectTaskExecutor:          NewMockSideEffectTaskExecutor[any, *TestSideEffectTask](controller),
		mockOutboundSideEffectTaskValidator: NewMockTaskValidator[any, TestOutboundSideEffectTask](controller),
		mockOutboundSideEffectTaskExecutor:  NewMockSideEffectTaskExecutor[any, TestOutboundSideEffectTask](controller),
		mockPureTaskValidator:               NewMockTaskValidator[any, *TestPureTask](controller),
		mockPureTaskExecutor:                NewMockPureTaskExecutor[any, *TestPureTask](controller),
	}
}

func (l *TestLibrary) Name() string {
	return "TestLibrary"
}

func (l *TestLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{
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
			l.mockSideEffectTaskValidator,
			l.mockSideEffectTaskExecutor,
		),
		NewRegistrableSideEffectTask(
			// NOTE this task is registered as a struct, instead of pointer to struct.
			"test_outbound_side_effect_task",
			l.mockOutboundSideEffectTaskValidator,
			l.mockOutboundSideEffectTaskExecutor,
		),
		NewRegistrablePureTask(
			"test_pure_task",
			l.mockPureTaskValidator,
			l.mockPureTaskExecutor,
		),
	}
}
