// Copyright (c) 2019 Uber Technologies, Inc.
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

package postgres

import "time"

var localZone, _ = time.Now().Zone()
var localOffset = getLocalOffset()

type (
	// DataConverter defines the API for conversions to/from
	// go types to postgres datatypes
	// TODO https://github.com/uber/cadence/issues/2892
	// There are some reasons:
	//r application layer is not consistent with timezone: for example,
	// in some case we write timestamp with local timezone but when the time.Time
	// is converted from "JSON"(from paging token), the timezone is missing
	DataConverter interface {
		ToPostgresDateTime(t time.Time) time.Time
		FromPostgresDateTime(t time.Time) time.Time
	}
	converter struct{}
)

// ToPostgresDateTime converts to time to Postgres datetime
func (c *converter) ToPostgresDateTime(t time.Time) time.Time {
	zn, _ := t.Zone()
	if zn != localZone {
		nano := t.UnixNano()
		t := time.Unix(0, nano)
		return t
	}
	return t
}

// FromPostgresDateTime converts postgres datetime and returns go time
func (c *converter) FromPostgresDateTime(t time.Time) time.Time {
	return t.Add(-localOffset)
}

func getLocalOffset() time.Duration {
	_, offsetSecs := time.Now().Zone()
	return time.Duration(offsetSecs) * time.Second
}
