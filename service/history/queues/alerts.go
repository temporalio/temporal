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

package queues

import (
	"strconv"

	"go.temporal.io/server/service/history/tasks"
)

type (
	// Alert is created by a Monitor when some statistics of the Queue is abnormal
	Alert struct {
		AlertType                  AlertType
		AlertAttributesReaderStuck *AlertAttributesReaderStuck
	}

	AlertType int

	AlertAttributesReaderStuck struct {
		ReaderID         int32
		CurrentWatermark tasks.Key
	}
)

const (
	AlertTypeUnspecified AlertType = iota
	AlertTypeReaderStuck
)

var (
	alertTypeNames = map[AlertType]string{
		0: "Unspecified",
		1: "ReaderStuck",
	}
)

func (a AlertType) String() string {
	s, ok := alertTypeNames[a]
	if ok {
		return s
	}
	return strconv.Itoa(int(a))
}
