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

package tag

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"go.temporal.io/server/common/primitives/timestamp"
)

// Tag is the interface for logging system
type Tag struct {
	// keep this Field private
	Field zap.Field
}

func NewBinaryTag(key string, value []byte) Tag {
	return Tag{
		Field: zap.Binary(key, value),
	}
}

func NewStringTag(key string, value string) Tag {
	return Tag{
		Field: zap.String(key, value),
	}
}

func NewInt64(key string, value int64) Tag {
	return Tag{
		Field: zap.Int64(key, value),
	}
}

func NewInt(key string, value int) Tag {
	return Tag{
		Field: zap.Int(key, value),
	}
}

func NewInt32(key string, value int32) Tag {
	return Tag{
		Field: zap.Int32(key, value),
	}
}

func NewBoolTag(key string, value bool) Tag {
	return Tag{
		Field: zap.Bool(key, value),
	}
}

func NewErrorTag(key string, value error) Tag {
	//NOTE zap already chosen "error" as key
	return Tag{
		Field: zap.Error(value),
	}
}

func NewDurationTag(key string, value time.Duration) Tag {
	return Tag{
		Field: zap.Duration(key, value),
	}
}

func NewDurationPtrTag(key string, value *time.Duration) Tag {
	return Tag{
		Field: zap.Duration(key, timestamp.DurationValue(value)),
	}
}

func NewTimeTag(key string, value time.Time) Tag {
	return Tag{
		Field: zap.Time(key, value),
	}
}

func NewTimePtrTag(key string, value *time.Time) Tag {
	return Tag{
		Field: zap.Time(key, timestamp.TimeValue(value)),
	}
}

func NewObjectTag(key string, value interface{}) Tag {
	return Tag{
		Field: zap.String(key, fmt.Sprintf("%v", value)),
	}
}

func NewPredefinedStringTag(key string, value string) Tag {
	return Tag{
		Field: zap.String(key, value),
	}
}
