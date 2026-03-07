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
	"net"

	SIMAPI "go.temporal.io/server/tools/gomad/api/lang"
)

type alias = string
type strct[N int64 | float64] struct{}

func fixture_maps() {
	SIMAPI.FuncStart()
	m := SIMAPI.MapInit[string, int64](string("A"), int64(0), string("B"), int64(1))

	m["A"] = 0
	SIMAPI.MapKey("A")
	m["B"], m["C"] = 1, 2
	SIMAPI.MapKey("C")
	SIMAPI.MapKey("B")

	m2 := SIMAPI.MapInit[[2]int, [3]int]([2]int{1, 1}, [3]int{1, 1, 1}, [2]int{2, 2}, [3]int{2, 2, 2})

	m3 := SIMAPI.MapInit[string, alias](string("A"), alias("0"), string("B"), alias("1"))

	m4 := SIMAPI.MapInit[int32, *net.IPNet](int32(0), &net.IPNet{
		IP: net.IP("<address>"),
	})

	m5 := SIMAPI.MapInit[string, interface{}](string("test"), net.IPNet{})

	m6 := SIMAPI.MapInitPtr[string, string]()

	var m7 map[strct[int64]]string
	m7[strct[int64]{}] = "s"
	SIMAPI.MapKey(strct[int64]{})

	var m8 map[string]map[int32]int64
	m8["s"][32] = 64
	SIMAPI.MapKey(32)
}
