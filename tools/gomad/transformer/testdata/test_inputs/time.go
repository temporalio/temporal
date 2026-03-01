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
	"time"
)

var timerFunc = time.NewTimer

type time_struct struct{ t *time.Timer }

func time_fixture() {
	now := time.Now()
	nowUTC := time.Now().UTC()
	_ = time.Since(t)

	var timer *time.Timer
	timer = time.NewTimer(2 * time.Second)
	timer.Stop()

	time.Sleep(2 * time.Second)

	<-time.After(2 * time.Minute)
	time.AfterFunc(time.Second, func() {})

	var ticker *time.Ticker
	ticker = time.NewTicker(500 * time.Millisecond)
	<-ticker.C

	// remains untouched:
	_ = time.Duration(0)
	t := time.Time{}
	_, _ = time.ParseDuration("...")

	_ = &time_struct{time.NewTimer(0)}
	_ = &time_struct{t: time.NewTimer(0)}
}

func time_func(t time.Timer)           {}
func time_ptr_func(t *time.Timer)      {}
func time_arr_func(t []*time.Timer)    {}
func time_arr2_func(t [][]*time.Timer) {}
func time_afterfunc() *time.Timer {
	return time.AfterFunc(time.Duration(0), func() {})
}
