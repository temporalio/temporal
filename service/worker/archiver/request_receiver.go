// Copyright (c) 2017 Uber Technologies, Inc.
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

package archiver

import (
	"go.uber.org/cadence/workflow"
)

type (
	// RequestReceiver is for receiving archive requests from a workflow signal channel
	RequestReceiver interface {
		Receive(workflow.Context, workflow.Channel) (interface{}, bool)
		ReceiveAsync(workflow.Channel) (interface{}, bool)
	}

	historyRequestReceiver struct{}
)

var (
	hReceiver = &historyRequestReceiver{}
)

// GetHistoryRequestReceiver returns a new receiver for archive history requests
func GetHistoryRequestReceiver() RequestReceiver {
	return hReceiver
}

func (r *historyRequestReceiver) Receive(ctx workflow.Context, ch workflow.Channel) (interface{}, bool) {
	var signal ArchiveHistoryRequest
	more := ch.Receive(ctx, &signal)
	return signal, more
}

func (r *historyRequestReceiver) ReceiveAsync(ch workflow.Channel) (interface{}, bool) {
	var signal ArchiveHistoryRequest
	ok := ch.ReceiveAsync(&signal)
	return signal, ok
}
