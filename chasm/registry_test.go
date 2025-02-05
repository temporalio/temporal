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

package chasm_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.uber.org/mock/gomock"
)

type (
	testTask1                  struct{}
	testTask2                  struct{}
	testTaskComponentInterface interface {
		DoSomething()
	}
)

func TestRegistry_RegisterComponents_Success(t *testing.T) {
	r := chasm.NewRegistry()
	ctrl := gomock.NewController(t)
	lib := chasm.NewMockLibrary(ctrl)
	lib.EXPECT().Name().Return("TestLibrary").AnyTimes()
	lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*chasm.MockComponent]("Component1"),
	})

	lib.EXPECT().Tasks().Return(nil)

	err := r.Register(lib)
	require.NoError(t, err)

	rc1, ok := r.Component("TestLibrary.Component1")
	require.True(t, ok)
	require.Equal(t, "Component1", rc1.Type())

	missingRC, ok := r.Component("TestLibrary.Component2")
	require.False(t, ok)
	require.Nil(t, missingRC)

	cInstance1 := chasm.NewMockComponent(ctrl)
	rc2, ok := r.ComponentFor(cInstance1)
	require.True(t, ok)
	require.Equal(t, "Component1", rc2.Type())

	cInstance2 := "invalid component instance"
	rc3, ok := r.ComponentFor(cInstance2)
	require.False(t, ok)
	require.Nil(t, rc3)
}

func TestRegistry_RegisterTasks_Success(t *testing.T) {
	r := chasm.NewRegistry()
	ctrl := gomock.NewController(t)
	lib := chasm.NewMockLibrary(ctrl)
	lib.EXPECT().Name().Return("TestLibrary").AnyTimes()
	lib.EXPECT().Components().Return(nil)

	lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
		chasm.NewRegistrableTask[*chasm.MockComponent, testTask1]("Task1", chasm.NewMockTaskHandler[*chasm.MockComponent, testTask1](ctrl)),
		chasm.NewRegistrableTask[testTaskComponentInterface, testTask2]("Task2", chasm.NewMockTaskHandler[testTaskComponentInterface, testTask2](ctrl)),
	})

	err := r.Register(lib)
	require.NoError(t, err)

	rt1, ok := r.Task("TestLibrary.Task1")
	require.True(t, ok)
	require.Equal(t, "Task1", rt1.Type())

	missingRT, ok := r.Task("TestLibrary.TaskMissing")
	require.False(t, ok)
	require.Nil(t, missingRT)

	tInstance1 := testTask2{}
	rt2, ok := r.TaskFor(tInstance1)
	require.True(t, ok)
	require.Equal(t, "Task2", rt2.Type())

	tInstance2 := "invalid task instance"
	rt3, ok := r.TaskFor(tInstance2)
	require.False(t, ok)
	require.Nil(t, rt3)
}

func TestRegistry_Register_LibraryError(t *testing.T) {
	ctrl := gomock.NewController(t)
	lib := chasm.NewMockLibrary(ctrl)

	t.Run("library name must not be empty", func(t *testing.T) {
		lib.EXPECT().Name().Return("")
		r := chasm.NewRegistry()
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must not be empty")
	})

	t.Run("library name must follow rules", func(t *testing.T) {
		lib.EXPECT().Name().Return("bad.lib.name")
		r := chasm.NewRegistry()
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must follow golang identifier rules")
	})
}

func TestRegistry_RegisterComponents_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	lib := chasm.NewMockLibrary(ctrl)
	lib.EXPECT().Name().Return("TestLibrary").AnyTimes()

	t.Run("component name must not be empty", func(t *testing.T) {
		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			chasm.NewRegistrableComponent[*chasm.MockComponent](""),
		})
		r := chasm.NewRegistry()
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must not be empty")
	})

	t.Run("component name must follow rules", func(t *testing.T) {
		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			chasm.NewRegistrableComponent[*chasm.MockComponent]("bad.component.name"),
		})
		r := chasm.NewRegistry()
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must follow golang identifier rules")
	})

	t.Run("component is already registered by name", func(t *testing.T) {
		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			chasm.NewRegistrableComponent[*chasm.MockComponent]("Component1"),
			chasm.NewRegistrableComponent[*chasm.MockComponent]("Component1"),
		})
		r := chasm.NewRegistry()
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already registered")
	})

	t.Run("component is already registered by type", func(t *testing.T) {
		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			chasm.NewRegistrableComponent[*chasm.MockComponent]("Component1"),
			chasm.NewRegistrableComponent[*chasm.MockComponent]("Component2"),
		})
		r := chasm.NewRegistry()

		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already registered")
	})

	t.Run("component must be a struct", func(t *testing.T) {
		lib.EXPECT().Components().Return([]*chasm.RegistrableComponent{
			chasm.NewRegistrableComponent[chasm.Component]("Component1"),
		})
		r := chasm.NewRegistry()

		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be struct or pointer to struct")
	})

}

func TestRegistry_RegisterTasks_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	lib := chasm.NewMockLibrary(ctrl)
	lib.EXPECT().Name().Return("TestLibrary").AnyTimes()
	lib.EXPECT().Components().Return(nil).AnyTimes()

	t.Run("task name must not be empty", func(t *testing.T) {
		r := chasm.NewRegistry()
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			chasm.NewRegistrableTask[*chasm.MockComponent, testTask1]("", chasm.NewMockTaskHandler[*chasm.MockComponent, testTask1](ctrl)),
		})
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must not be empty")
	})

	t.Run("task name must follow rules", func(t *testing.T) {
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			chasm.NewRegistrableTask[*chasm.MockComponent, testTask1]("bad.task.name", chasm.NewMockTaskHandler[*chasm.MockComponent, testTask1](ctrl)),
		})
		r := chasm.NewRegistry()
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "name must follow golang identifier rules")
	})

	t.Run("task is already registered by name", func(t *testing.T) {
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			chasm.NewRegistrableTask[*chasm.MockComponent, testTask1]("Task1", chasm.NewMockTaskHandler[*chasm.MockComponent, testTask1](ctrl)),
			chasm.NewRegistrableTask[*chasm.MockComponent, testTask1]("Task1", chasm.NewMockTaskHandler[*chasm.MockComponent, testTask1](ctrl)),
		})
		r := chasm.NewRegistry()
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already registered")
	})

	t.Run("task is already registered by type", func(t *testing.T) {
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			chasm.NewRegistrableTask[*chasm.MockComponent, testTask1]("Task1", chasm.NewMockTaskHandler[*chasm.MockComponent, testTask1](ctrl)),
			chasm.NewRegistrableTask[*chasm.MockComponent, testTask1]("Task2", chasm.NewMockTaskHandler[*chasm.MockComponent, testTask1](ctrl)),
		})
		r := chasm.NewRegistry()
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "is already registered")
	})

	t.Run("task component struct must implement Component", func(t *testing.T) {
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			// MockComponent has only pointer receivers and therefore does not implement Component interface.
			chasm.NewRegistrableTask[chasm.MockComponent, testTask1]("Task1", chasm.NewMockTaskHandler[chasm.MockComponent, testTask1](ctrl)),
		})
		r := chasm.NewRegistry()
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "struct that implements Component interface")
	})

	t.Run("task must be struct", func(t *testing.T) {
		lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
			chasm.NewRegistrableTask[*chasm.MockComponent, string]("Task1", chasm.NewMockTaskHandler[*chasm.MockComponent, string](ctrl)),
		})
		r := chasm.NewRegistry()
		err := r.Register(lib)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be struct or pointer to struct")
	})
}
