// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
