// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
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

package sqlite

import "time"

var (
	minSQLiteDateTime = getMinSQLiteDateTime()
)

type (
	// DataConverter defines the API for conversions to/from
	// go types to sqlite datatypes
	DataConverter interface {
		ToSQLiteDateTime(t time.Time) time.Time
		FromSQLiteDateTime(t time.Time) time.Time
	}
	converter struct{}
)

// ToSQLiteDateTime converts to time to SQLite datetime
func (c *converter) ToSQLiteDateTime(t time.Time) time.Time {
	if t.IsZero() {
		return minSQLiteDateTime
	}
	return t.UTC()
}

// FromSQLiteDateTime converts SQLite datetime and returns go time
func (c *converter) FromSQLiteDateTime(t time.Time) time.Time {
	if t.Equal(minSQLiteDateTime) {
		return time.Time{}.UTC()
	}
	return t.UTC()
}

func getMinSQLiteDateTime() time.Time {
	t, err := time.Parse(time.RFC3339, "1000-01-01T00:00:00Z")
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return t.UTC()
}
