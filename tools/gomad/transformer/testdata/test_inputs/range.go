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
)

func forRange() {
	var m map[int]string

	for range m {
		fmt.Printf("[1]")
	}

	for _ = range m {
		fmt.Printf("[2]")
	}

	for _, _ = range m {
		fmt.Printf("[3]")
	}

	for k := range m {
		fmt.Printf("[4] %v", k)
	}

	for k, _ := range m {
		fmt.Printf("[5] %v", k)
	}

	for _, v := range m {
		v := v
		fmt.Printf("[6] %v", v)
	}

	for k, v := range m {
		fmt.Printf("[7] %v %v", k, v)
	}

ForLabel:
	for k, v := range m {
		fmt.Printf("[8] %v %v", k, v)
		break ForLabel
	}

	for k, v := range map[string]int{"A": 0} {
		fmt.Printf("[9] %v %v", k, v)
	}

	var k int
	var v string
	for k, v = range m {
		fmt.Printf("[10] %v %v", k, v)
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

	for range c {
	}

	for v := range c {
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
