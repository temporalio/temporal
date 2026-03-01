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
	"os"
	"time"
)

type myErr error

func select_fixture() {
	var s string
	customTypeChan := make(chan myErr)
	extTypeChan := make(chan *os.ProcAttr)
	basicTypeChan := make(chan string)
	nestedTypeChan := make(chan chan string)
	sliceTypeChan := make(chan []*os.ProcAttr)
	funcChan := make(chan []func())

	select {
	case <-customTypeChan:
		fmt.Println("1")
	case s = <-basicTypeChan:
		fmt.Println("2", s)
	case v, ok := <-customTypeChan:
		fmt.Println("3", v, ok)
	case basicTypeChan <- "abort":
		fmt.Println("4")
	case v := <-time.After(3 * time.Second):
		fmt.Println("5", v)
	case v := <-extTypeChan:
		fmt.Println("6", v)
	case v := <-nestedTypeChan:
		fmt.Println("7", v)
	case v := <-sliceTypeChan:
		fmt.Println("8", v)
	case v := <-funcChan:
		fmt.Println("9", v)
	case _ = <-basicTypeChan:
		fmt.Println("10")
	default:
		fmt.Println("11")
	}

Breakpoint:
	select {
	case <-customTypeChan:
		select {
		case <-customTypeChan:
			break Breakpoint
		}
	}

	select {
	default:
	}

	select {
	default:
	case <-customTypeChan:
	}
}
