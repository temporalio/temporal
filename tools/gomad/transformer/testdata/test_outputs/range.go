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
	"fmt"

	SIMAPI "go.temporal.io/server/tools/gomad/api/lang"
)

func forRange() {
	SIMAPI.FuncStart()
	var m map[int]string
	for _, _ = range SIMAPI.MapKeys(m) {
		fmt.Printf("[1]")
	}
	for _, _ = range SIMAPI.MapKeys(m) {
		fmt.Printf("[2]")
	}
	for _, _ = range SIMAPI.MapKeys(m) {
		fmt.Printf("[3]")
	}
	for _, k := range SIMAPI.MapKeys(m) {
		fmt.Printf("[4] %v", k)
	}
	for _, k := range SIMAPI.MapKeys(m) {
		fmt.Printf("[5] %v", k)
	}
	for _, SIMRangeKey := range SIMAPI.MapKeys(m) {
		v := m[SIMRangeKey]
		{
			v := v
			fmt.Printf("[6] %v", v)
		}
	}
	for _, k := range SIMAPI.MapKeys(m) {
		v := m[k]
		{
			fmt.Printf("[7] %v %v", k, v)
		}
	}

ForLabel:
	for _, k := range SIMAPI.MapKeys(m) {
		v := m[k]
		{
			fmt.Printf("[8] %v %v", k, v)
			break ForLabel
		}
	}
	SIMRangeSrc0 := SIMAPI.MapInit[string, int](string("A"), int(0))
	for _, k := range SIMAPI.MapKeys(SIMRangeSrc0) {
		v := SIMRangeSrc0[k]
		{
			fmt.Printf("[9] %v %v", k, v)
		}
	}

	var k int
	var v string
	for _, k = range SIMAPI.MapKeys(m) {
		v = m[k]
		{
			fmt.Printf("[10] %v %v", k, v)
		}
	}

	var l []int

	for v := range l {
		fmt.Println(v)
	}

	for i, v := range l {
		fmt.Println(i, v)
	}

	for _, v := range l {
		fmt.Println(v)
	}

	var c chan any
	for {
		_, ok := SIMAPI.ChanRcvOk(c)
		if !ok {
			break
		}
	}
	for {

		v, ok := SIMAPI.ChanRcvOk(c)
		if !ok {
			break
		}
		fmt.Println(v)
	}

	var s string

	for char := range s {
		fmt.Println(char)
	}

	for char := range "hello" {
		fmt.Println(char)
	}
}
