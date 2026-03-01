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

//go:build fixture

package fixtures

import (
	"sync/atomic"

	SIMAPI "gomad.local/go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "gomad.local/go.temporal.io/server/tools/gomad/api/lib"
)

func atomic_fixture() {
	SIMAPI.FuncStart()
	var b SIMLIB.Bool
	b.Store(true)
	_ = b.Load()

	var i SIMLIB.Int32
	i.Store(1)
	_ = i.Add(1)

	var u32 SIMLIB.AtomicUint32
	u32.Store(1)
	_ = u32.Add(1)

	var u64 SIMLIB.AtomicUint64
	u64.Store(1)
	_ = u64.Add(1)

	var p SIMLIB.Pointer[int]
	x := 42
	p.Store(&x)
	_ = p.Load()

	var v SIMLIB.Value
	v.Store(42)
	_ = v.Load()
}

type _ = atomic.Bool
type _ = atomic.Int32
type _ = atomic.Pointer
type _ = atomic.Uint32
type _ = atomic.Uint64
type _ = atomic.Value
