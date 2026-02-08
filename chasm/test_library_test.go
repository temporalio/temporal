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
	return testLibraryName
}

func (l *TestLibrary) Components() []*RegistrableComponent {
	return []*RegistrableComponent{
		NewRegistrableComponent[*TestComponent](
			testComponentName,
			WithBusinessIDAlias("TestBusinessId"),
			WithSearchAttributes(TestComponentStartTimeSearchAttribute),
		),
		NewRegistrableComponent[*TestSubComponent1](testSubComponent1Name),
		NewRegistrableComponent[*TestSubComponent11](testSubComponent11Name),
		NewRegistrableComponent[*TestSubComponent2](testSubComponent2Name),
	}
}

func (l *TestLibrary) Tasks() []*RegistrableTask {
	return []*RegistrableTask{
		NewRegistrableSideEffectTask(
			testSideEffectTaskName,
			l.mockSideEffectTaskValidator,
			l.mockSideEffectTaskExecutor,
		),
		NewRegistrableSideEffectTask(
			// NOTE this task is registered as a struct, instead of pointer to struct.
			testOutboundSideEffectTaskName,
			l.mockOutboundSideEffectTaskValidator,
			l.mockOutboundSideEffectTaskExecutor,
		),
		NewRegistrablePureTask(
			testPureTaskName,
			l.mockPureTaskValidator,
			l.mockPureTaskExecutor,
		),
	}
}
