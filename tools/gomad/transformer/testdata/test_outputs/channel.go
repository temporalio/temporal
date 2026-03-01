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
	"net"

	SIMAPI "gomad.local/go.temporal.io/server/tools/gomad/api/lang"
)

type chan_type chan bool
type chan_struct struct {
	c chan error
}
type chan_itfc interface {
	Chan() chan bool
}
type generic[T any] struct {
	val T
}

type alias func() int
type mystruct struct {
	chan_alias chan func() int
}

func channel_fixture() {
	SIMAPI.FuncStart()
	var unbuffered chan string
	unbuffered = make(chan string)
	SIMAPI.ChanSend(unbuffered, "ping")
	SIMAPI.ChanRcv(unbuffered)
	_ = SIMAPI.ChanRcv(unbuffered)
	_, ok := SIMAPI.ChanRcvOk(unbuffered)
	fmt.Println(ok)

	buffered := make(chan int64, 42)
	SIMAPI.ChanClose(buffered)
	defer SIMAPI.ChanClose(buffered)

	unbuffered = nil

	chanList := make([]chan *string, 2)
	clear(chanList)

	var c any
	c = make(chan chan string)
	_ = c.(chan chan string)

	var ct chan_type
	ct = make(chan_type)
	_ = len(ct)
	_ = cap(ct)

	var gc chan generic[string]
	SIMAPI.ChanSend(gc, generic[string]{val: "test"})
	SIMAPI.ChanSend(chan_struct{}.c, net.ErrClosed)
}

func chan_func(a []chan bool) {}

func (c chan_type) Wait() { SIMAPI.FuncStart(); SIMAPI.ChanRcv(c) }
func (c chan_type) Done() { SIMAPI.FuncStart(); SIMAPI.ChanSend(c, true) }
