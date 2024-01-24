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

package effect

import "context"

// Buffer holds a set of effect and rollback functions that can be invoked as a
// batch with a defined order. Once either Apply or Cancel is called, all
// buffered effects are cleared. This type is not threadsafe. The zero-value of
// a Buffer is a valid state.
type Buffer struct {
	effects []func(context.Context)
	cancels []func(context.Context)
}

// OnAfterCommit adds the provided effect function to set of such functions to
// be invoked when Buffer.Apply is called.
func (b *Buffer) OnAfterCommit(effect func(context.Context)) {
	b.effects = append(b.effects, effect)
}

// OnAfterRollback adds the provided effect function to set of such functions to
// be invoked when Buffer.Cancel is called.
func (b *Buffer) OnAfterRollback(effect func(context.Context)) {
	b.cancels = append(b.cancels, effect)
}

// Apply invokes the buffered effect functions in the order that they were added
// to this Buffer.
func (b *Buffer) Apply(ctx context.Context) {
	b.cancels = nil
	for _, effect := range b.effects {
		effect(ctx)
	}
	b.effects = nil
}

// Cancel invokes the buffered rollback functions in the reverse of the order
// that they were added to this Buffer.
func (b *Buffer) Cancel(ctx context.Context) {
	b.effects = nil
	for i := len(b.cancels) - 1; i >= 0; i-- {
		b.cancels[i](ctx)
	}
	b.cancels = nil
}
