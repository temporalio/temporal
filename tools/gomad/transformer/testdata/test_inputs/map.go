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

import "net"

type alias = string
type strct[N int64 | float64] struct{}

func fixture_maps() {
	m := map[string]int64{
		"A": 0,
		"B": 1,
	}
	m["A"] = 0
	m["B"], m["C"] = 1, 2

	m2 := map[[2]int][3]int{
		{1, 1}: {1, 1, 1},
		{2, 2}: {2, 2, 2},
	}
	m3 := map[string]alias{
		"A": "0",
		"B": "1",
	}
	m4 := map[int32]*net.IPNet{
		0: {
			IP: net.IP("<address>"),
		},
	}
	m5 := map[string]interface{}{
		"test": net.IPNet{},
	}
	m6 := &map[string]string{}

	var m7 map[strct[int64]]string
	m7[strct[int64]{}] = "s"

	var m8 map[string]map[int32]int64
	m8["s"][32] = 64
}
