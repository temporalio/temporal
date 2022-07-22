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

package tdbg

import (
	"time"
)

const (
	localHostPort = "127.0.0.1:7233"

	// regex expression for parsing time durations, shorter, longer notations and numeric value respectively
	defaultDateTimeRangeShortRE = "^[1-9][0-9]*[smhdwMy]$"                                // eg. 1s, 20m, 300h etc.
	defaultDateTimeRangeLongRE  = "^[1-9][0-9]*(second|minute|hour|day|week|month|year)$" // eg. 1second, 20minute, 300hour etc.
	defaultDateTimeRangeNum     = "^[1-9][0-9]*"                                          // eg. 1, 20, 300 etc.

	// time ranges
	day   = 24 * time.Hour
	week  = 7 * day
	month = 30 * day
	year  = 365 * day

	defaultTimeFormat              = "15:04:05"   // used for converting UnixNano to string like 16:16:36 (only time)
	defaultDateTimeFormat          = time.RFC3339 // used for converting UnixNano to string like 2018-02-15T16:16:36-08:00
	defaultContextTimeoutInSeconds = 5
	defaultContextTimeout          = defaultContextTimeoutInSeconds * time.Second

	showErrorStackEnv = `TEMPORAL_CLI_SHOW_STACKS`
)

var (
	cFactory ClientFactory
)
