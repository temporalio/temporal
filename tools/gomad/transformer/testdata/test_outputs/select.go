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

	SIMAPI "gomad.local/go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "gomad.local/go.temporal.io/server/tools/gomad/api/lib"
)

type myErr error

func select_fixture() {
	SIMAPI.FuncStart()
	var s string
	customTypeChan := make(chan myErr)
	extTypeChan := make(chan *os.ProcAttr)
	basicTypeChan := make(chan string)
	nestedTypeChan := make(chan chan string)
	sliceTypeChan := make(chan []*os.ProcAttr)
	funcChan := make(chan []func())
	switch selector := SIMAPI.Select(0, SIMAPI.RcvChan(customTypeChan), nil, 0, SIMAPI.RcvChan(basicTypeChan), nil, 0, SIMAPI.RcvChan(customTypeChan), nil, 1, SIMAPI.SndChan(basicTypeChan), func() any {
		return "abort"
	}, 0, SIMAPI.RcvChan(SIMLIB.After(3*time.Second)), nil, 0, SIMAPI.RcvChan(extTypeChan), nil, 0, SIMAPI.RcvChan(nestedTypeChan), nil, 0, SIMAPI.RcvChan(sliceTypeChan), nil, 0, SIMAPI.RcvChan(funcChan), nil, 0, SIMAPI.RcvChan(basicTypeChan), nil, nil); selector.Case {
	case 0:
		fmt.Println("1")
	case 1:
		selector.Assign(&s)
		fmt.Println("2", s)
	case 2:
		var v myErr
		selector.Assign(&v)
		ok := selector.Ok
		fmt.Println("3", v, ok)
	case 3:

		fmt.Println("4")
	case 4:
		var v time.Time
		selector.Assign(&v)
		fmt.Println("5", v)
	case 5:
		var v *os.ProcAttr
		selector.Assign(&v)
		fmt.Println("6", v)
	case 6:
		var v chan string
		selector.Assign(&v)
		fmt.Println("7", v)
	case 7:
		var v []*os.ProcAttr
		selector.Assign(&v)
		fmt.Println("8", v)
	case 8:
		var v []func()
		selector.Assign(&v)
		fmt.Println("9", v)
	case 9:

		fmt.Println("10")
	default:

		fmt.Println("11")
	}

Breakpoint:
	switch selector := SIMAPI.Select(0, SIMAPI.RcvChan(customTypeChan), nil); selector.Case {
	case 0:
		switch selector := SIMAPI.Select(0, SIMAPI.RcvChan(customTypeChan), nil); selector.Case {
		case 0:
			break Breakpoint
		default:
			panic("unreachable")
		}
	default:
		panic("unreachable")
	}
	switch selector := SIMAPI.Select(nil); selector.Case {
	default:
	}
	switch selector := SIMAPI.Select(0, SIMAPI.RcvChan(customTypeChan), nil, nil); selector.Case {
	case 0:
	default:
	}

}

var _ = time.After
