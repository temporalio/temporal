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

package tag

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Tag is the interface for logging system
type Tag struct {
	// keep this field private
	field zap.Field
}

// Field returns a zap field
func (t *Tag) Field() zap.Field {
	return t.field
}

func newStringTag(key string, value string) Tag {
	return Tag{
		field: zap.String(key, value),
	}
}

func newInt64(key string, value int64) Tag {
	return Tag{
		field: zap.Int64(key, value),
	}
}

func newInt(key string, value int) Tag {
	return Tag{
		field: zap.Int(key, value),
	}
}

func newInt32(key string, value int32) Tag {
	return Tag{
		field: zap.Int32(key, value),
	}
}

func newBoolTag(key string, value bool) Tag {
	return Tag{
		field: zap.Bool(key, value),
	}
}

func newErrorTag(key string, value error) Tag {
	//NOTE zap already chosen "error" as key
	return Tag{
		field: zap.Error(value),
	}
}

func newDurationTag(key string, value time.Duration) Tag {
	return Tag{
		field: zap.Duration(key, value),
	}
}

func newTimeTag(key string, value time.Time) Tag {
	return Tag{
		field: zap.Time(key, value),
	}
}

func newObjectTag(key string, value interface{}) Tag {
	return Tag{
		field: zap.String(key, fmt.Sprintf("%v", value)),
	}
}

func newPredefinedStringTag(key string, value string) Tag {
	return Tag{
		field: zap.String(key, value),
	}
}
