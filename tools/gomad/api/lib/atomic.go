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

package lib

import (
	"sync/atomic"

	SIM "go.temporal.io/server/tools/gomad/runtime"
)

// Atomic wrappers add a cooperative yield before every operation so that
// spin-loops (for !v.Load() {}) do not hang the simulation — the yield
// lets the scheduler run other goroutines, enabling the goroutine that
// calls Store/Add/etc. to make progress.

type Bool struct{ v atomic.Bool }

func (b *Bool) Load() bool                        { SIM.Yield(); return b.v.Load() }
func (b *Bool) Store(val bool)                    { SIM.Yield(); b.v.Store(val) }
func (b *Bool) Swap(new bool) bool                { SIM.Yield(); return b.v.Swap(new) }
func (b *Bool) CompareAndSwap(old, new bool) bool { SIM.Yield(); return b.v.CompareAndSwap(old, new) }

type Int32 struct{ v atomic.Int32 }

func (x *Int32) Load() int32                        { SIM.Yield(); return x.v.Load() }
func (x *Int32) Store(val int32)                    { SIM.Yield(); x.v.Store(val) }
func (x *Int32) Swap(new int32) int32               { SIM.Yield(); return x.v.Swap(new) }
func (x *Int32) Add(delta int32) int32              { SIM.Yield(); return x.v.Add(delta) }
func (x *Int32) CompareAndSwap(old, new int32) bool { SIM.Yield(); return x.v.CompareAndSwap(old, new) }

type Int64 struct{ v atomic.Int64 }

func (x *Int64) Load() int64                        { SIM.Yield(); return x.v.Load() }
func (x *Int64) Store(val int64)                    { SIM.Yield(); x.v.Store(val) }
func (x *Int64) Swap(new int64) int64               { SIM.Yield(); return x.v.Swap(new) }
func (x *Int64) Add(delta int64) int64              { SIM.Yield(); return x.v.Add(delta) }
func (x *Int64) CompareAndSwap(old, new int64) bool { SIM.Yield(); return x.v.CompareAndSwap(old, new) }

// AtomicUint32 and AtomicUint64 use prefixed names to avoid conflicts with
// the math/rand wrappers Uint32() and Uint64() in the same package.

type AtomicUint32 struct{ v atomic.Uint32 }

func (x *AtomicUint32) Load() uint32                          { SIM.Yield(); return x.v.Load() }
func (x *AtomicUint32) Store(val uint32)                      { SIM.Yield(); x.v.Store(val) }
func (x *AtomicUint32) Swap(new uint32) uint32                { SIM.Yield(); return x.v.Swap(new) }
func (x *AtomicUint32) Add(delta uint32) uint32               { SIM.Yield(); return x.v.Add(delta) }
func (x *AtomicUint32) CompareAndSwap(old, new uint32) bool   { SIM.Yield(); return x.v.CompareAndSwap(old, new) }

type AtomicUint64 struct{ v atomic.Uint64 }

func (x *AtomicUint64) Load() uint64                          { SIM.Yield(); return x.v.Load() }
func (x *AtomicUint64) Store(val uint64)                      { SIM.Yield(); x.v.Store(val) }
func (x *AtomicUint64) Swap(new uint64) uint64                { SIM.Yield(); return x.v.Swap(new) }
func (x *AtomicUint64) Add(delta uint64) uint64               { SIM.Yield(); return x.v.Add(delta) }
func (x *AtomicUint64) CompareAndSwap(old, new uint64) bool   { SIM.Yield(); return x.v.CompareAndSwap(old, new) }

type Uintptr struct{ v atomic.Uintptr }

func (x *Uintptr) Load() uintptr                          { SIM.Yield(); return x.v.Load() }
func (x *Uintptr) Store(val uintptr)                      { SIM.Yield(); x.v.Store(val) }
func (x *Uintptr) Swap(new uintptr) uintptr               { SIM.Yield(); return x.v.Swap(new) }
func (x *Uintptr) Add(delta uintptr) uintptr              { SIM.Yield(); return x.v.Add(delta) }
func (x *Uintptr) CompareAndSwap(old, new uintptr) bool   { SIM.Yield(); return x.v.CompareAndSwap(old, new) }

type Pointer[T any] struct{ v atomic.Pointer[T] }

func (p *Pointer[T]) Load() *T                          { SIM.Yield(); return p.v.Load() }
func (p *Pointer[T]) Store(val *T)                      { SIM.Yield(); p.v.Store(val) }
func (p *Pointer[T]) Swap(new *T) *T                    { SIM.Yield(); return p.v.Swap(new) }
func (p *Pointer[T]) CompareAndSwap(old, new *T) bool   { SIM.Yield(); return p.v.CompareAndSwap(old, new) }

type Value struct{ v atomic.Value }

func (x *Value) Load() any                          { SIM.Yield(); return x.v.Load() }
func (x *Value) Store(val any)                      { SIM.Yield(); x.v.Store(val) }
func (x *Value) Swap(new any) any                   { SIM.Yield(); return x.v.Swap(new) }
func (x *Value) CompareAndSwap(old, new any) bool   { SIM.Yield(); return x.v.CompareAndSwap(old, new) }
