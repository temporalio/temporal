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

package chasm

import (
	"reflect"
)

type (
	RegistrableTask struct {
		taskType        string
		library         namer
		goType          reflect.Type
		componentGoType reflect.Type // It is not clear how this one is used.
		handler         any
	}

	RegistrableTaskOption func(*RegistrableTask)
)

// NOTE: C is not Component but any.
func NewRegistrableTask[C any, T any](
	taskType string,
	handler TaskHandler[C, T],
	opts ...RegistrableTaskOption,
) *RegistrableTask {
	rt := &RegistrableTask{
		taskType:        taskType,
		goType:          reflect.TypeFor[T](),
		componentGoType: reflect.TypeFor[C](),
		handler:         handler,
	}
	for _, opt := range opts {
		opt(rt)
	}
	return rt
}

// fqType returns the fully qualified name of the task, which is a combination of
// the library name and the task type. This is used to uniquely identify
// the task in the registry.
func (rt RegistrableTask) fqType() string {
	if rt.library == nil {
		// this should never happen because the task is only accessible from the library.
		panic("task is not registered to a library")
	}
	return fullyQualifiedName(rt.library.Name(), rt.taskType)
}
