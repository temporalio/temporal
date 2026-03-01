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

package lang

import (
	"unsafe"

	SIM "go.temporal.io/server/tools/gomad/runtime"
	"go.temporal.io/server/tools/gomad/util/verify"
)

func ChanSend[T any](ch chan<- T, msg any) {
	verify.T(ch != nil, "sending to a nil channel") // blocks forever and is most likely a bug

	SIM.GetOrCreateChan[T](*(*chan T)(unsafe.Pointer(&ch))).Snd(msg)
}

func ChanRcv[T any](ch <-chan T) T {
	res, _ := ChanRcvOk(ch)
	return res
}

func ChanRcvOk[T any](ch <-chan T) (T, bool) {
	verify.T(ch != nil, "receiving from a nil channel") // blocks forever and is most likely a bug

	msg, ok := SIM.GetOrCreateChan[T](*(*chan T)(unsafe.Pointer(&ch))).RcvOk()

	var typedMsg T
	if msg != nil { // casting nil panics
		typedMsg = msg.(T)
	}
	return typedMsg, ok
}

func ChanClose[T any](ch chan<- T) {
	SIM.GetOrCreateChan(*(*chan T)(unsafe.Pointer(&ch))).Cls()
}

func RcvChan[T any](ch <-chan T) SIM.Channel {
	return SIM.GetOrCreateChan(*(*chan T)(unsafe.Pointer(&ch)))
}

func SndChan[T any](ch chan<- T) SIM.Channel {
	return SIM.GetOrCreateChan(*(*chan T)(unsafe.Pointer(&ch)))
}
