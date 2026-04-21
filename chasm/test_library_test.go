// TODO: move this to chasm_test package
package chasm

import (
	"go.uber.org/mock/gomock"
)

type TestLibrary struct {
	UnimplementedLibrary

	controller *gomock.Controller

	mockSideEffectTaskHandler         *MockSideEffectTaskHandler[any, *TestSideEffectTask]
	mockDiscardableSideEffectHandler  *MockSideEffectTaskHandler[any, *TestDiscardableSideEffectTask]
	mockOutboundSideEffectTaskHandler *MockSideEffectTaskHandler[any, TestOutboundSideEffectTask]
	mockPureTaskHandler               *MockPureTaskHandler[any, *TestPureTask]
}

func newTestLibrary(
	controller *gomock.Controller,
) *TestLibrary {
	return &TestLibrary{
		controller: controller,

		mockSideEffectTaskHandler:         NewMockSideEffectTaskHandler[any, *TestSideEffectTask](controller),
		mockDiscardableSideEffectHandler:  NewMockSideEffectTaskHandler[any, *TestDiscardableSideEffectTask](controller),
		mockOutboundSideEffectTaskHandler: NewMockSideEffectTaskHandler[any, TestOutboundSideEffectTask](controller),
		mockPureTaskHandler:               NewMockPureTaskHandler[any, *TestPureTask](controller),
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
		NewRegistrableComponent[*TestSkipIfCleanComponent](
			testSkipIfCleanComponentName,
			WithSkipPersistenceIfClean(),
		),
	}
}

func (l *TestLibrary) Tasks() []*RegistrableTask {
	return []*RegistrableTask{
		NewRegistrableSideEffectTask(
			testSideEffectTaskName,
			l.mockSideEffectTaskHandler,
		),
		NewRegistrableSideEffectTask(
			testDiscardableSideEffectTaskName,
			l.mockDiscardableSideEffectHandler,
		),
		NewRegistrableSideEffectTask(
			// NOTE this task is registered as a struct, instead of pointer to struct.
			testOutboundSideEffectTaskName,
			l.mockOutboundSideEffectTaskHandler,
		),
		NewRegistrablePureTask(
			testPureTaskName,
			l.mockPureTaskHandler,
		),
	}
}
