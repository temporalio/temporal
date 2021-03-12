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
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	// ZapTag is the wrapper over zap.Field.
	ZapTag struct {
		// keep this field private
		field zap.Field
	}
)

// NewZapTag creates new ZapTag from zap.Field.
func NewZapTag(field zap.Field) ZapTag {
	return ZapTag{
		field: field,
	}
}

func (t ZapTag) Field() zap.Field {
	return t.field
}

func (t ZapTag) Key() string {
	return t.field.Key
}

func (t ZapTag) Value() interface{} {
	// Not for production use.
	enc := zapcore.NewMapObjectEncoder()
	t.field.AddTo(enc)
	for _, val := range enc.Fields {
		return val
	}
	return nil
}

func NewBinaryTag(key string, value []byte) ZapTag {
	return ZapTag{
		field: zap.Binary(key, value),
	}
}

func NewStringTag(key string, value string) ZapTag {
	return ZapTag{
		field: zap.String(key, value),
	}
}

func NewStringsTag(key string, value []string) ZapTag {
	return ZapTag{
		field: zap.Strings(key, value),
	}
}

func NewInt64(key string, value int64) ZapTag {
	return ZapTag{
		field: zap.Int64(key, value),
	}
}

func NewInt(key string, value int) ZapTag {
	return ZapTag{
		field: zap.Int(key, value),
	}
}

func NewInt32(key string, value int32) ZapTag {
	return ZapTag{
		field: zap.Int32(key, value),
	}
}

func NewBoolTag(key string, value bool) ZapTag {
	return ZapTag{
		field: zap.Bool(key, value),
	}
}

func NewErrorTag(value error) ZapTag {
	// NOTE: zap already chosen "error" as key
	return ZapTag{
		field: zap.Error(value),
	}
}

func NewDurationTag(key string, value time.Duration) ZapTag {
	return ZapTag{
		field: zap.Duration(key, value),
	}
}

func NewDurationPtrTag(key string, value *time.Duration) ZapTag {
	return ZapTag{
		field: zap.Duration(key, timestamp.DurationValue(value)),
	}
}

func NewTimeTag(key string, value time.Time) ZapTag {
	return ZapTag{
		field: zap.Time(key, value),
	}
}

func NewTimePtrTag(key string, value *time.Time) ZapTag {
	return ZapTag{
		field: zap.Time(key, timestamp.TimeValue(value)),
	}
}

func NewAnyTag(key string, value interface{}) ZapTag {
	return ZapTag{
		field: zap.Any(key, value),
	}
}
