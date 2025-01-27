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

type Registry struct{}

func (r *Registry) RegisterLibrary(lib Library) {
	panic("not implemented")
}

type Library interface {
	Name() string
	Components() []RegistrableComponent
	Tasks() []RegistrableTask
	// Service()

	mustEmbedUnimplementedLibrary()
}

type UnimplementedLibrary struct{}

func (UnimplementedLibrary) Components() []RegistrableComponent {
	return nil
}

func (UnimplementedLibrary) Tasks() []RegistrableTask {
	return nil
}

func (UnimplementedLibrary) mustEmbedUnimplementedLibrary() {}

type RegistrableComponent struct {
}

func NewRegistrableComponent[C Component](
	name string,
	opts ...RegistrableComponentOption,
) RegistrableComponent {
	panic("not implemented")
}

type RegistrableComponentOption func(*RegistrableComponent)

func EntityEphemeral() RegistrableComponentOption {
	panic("not implemented")
}

// Is there any use case where we don't want to replicate
// certain instances of a archetype?
func EntitySingleCluster() RegistrableComponentOption {
	panic("not implemented")
}

func EntityShardingFn(
	func(EntityKey) string,
) RegistrableComponentOption {
	panic("not implemented")
}

type RegistrableTask struct{}

func NewRegistrableTask[C any, T any](
	name string,
	handler TaskHandler[C, T],
	// opts ...RegistrableTaskOptions, no options right now
) RegistrableTask {
	panic("not implemented")
}
