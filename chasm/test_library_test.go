// TODO: move this to chasm_test package
package chasm

import (
	"context"

	"go.uber.org/mock/gomock"
)

// mockDiscardableSideEffectExecutor wraps MockSideEffectTaskExecutor and adds SideEffectDiscardHandler support for
// testing ExecuteSideEffectDiscardTask.
type mockDiscardableSideEffectExecutor struct {
	*MockSideEffectTaskExecutor[any, *TestDiscardableSideEffectTask]
	handleDiscardFn func(ctx context.Context, ref ComponentRef, attrs TaskAttributes, task any) error
}

func (m *mockDiscardableSideEffectExecutor) HandleDiscard(
	ctx context.Context,
	ref ComponentRef,
	attrs TaskAttributes,
	task any,
) error {
	return m.handleDiscardFn(ctx, ref, attrs, task)
}

type TestLibrary struct {
	UnimplementedLibrary

	controller *gomock.Controller

	mockSideEffectTaskValidator            *MockTaskValidator[any, *TestSideEffectTask]
	mockSideEffectTaskExecutor             *MockSideEffectTaskExecutor[any, *TestSideEffectTask]
	mockDiscardableSideEffectTaskValidator *MockTaskValidator[any, *TestDiscardableSideEffectTask]
	mockDiscardableSideEffectExecutor      *mockDiscardableSideEffectExecutor
	mockOutboundSideEffectTaskValidator    *MockTaskValidator[any, TestOutboundSideEffectTask]
	mockOutboundSideEffectTaskExecutor     *MockSideEffectTaskExecutor[any, TestOutboundSideEffectTask]
	mockPureTaskValidator                  *MockTaskValidator[any, *TestPureTask]
	mockPureTaskExecutor                   *MockPureTaskExecutor[any, *TestPureTask]
}

func newTestLibrary(
	controller *gomock.Controller,
) *TestLibrary {
	return &TestLibrary{
		controller: controller,

		mockSideEffectTaskValidator:            NewMockTaskValidator[any, *TestSideEffectTask](controller),
		mockSideEffectTaskExecutor:             NewMockSideEffectTaskExecutor[any, *TestSideEffectTask](controller),
		mockDiscardableSideEffectTaskValidator: NewMockTaskValidator[any, *TestDiscardableSideEffectTask](controller),
		mockDiscardableSideEffectExecutor: &mockDiscardableSideEffectExecutor{
			MockSideEffectTaskExecutor: NewMockSideEffectTaskExecutor[any, *TestDiscardableSideEffectTask](controller),
		},
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
			testDiscardableSideEffectTaskName,
			l.mockDiscardableSideEffectTaskValidator,
			l.mockDiscardableSideEffectExecutor,
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
