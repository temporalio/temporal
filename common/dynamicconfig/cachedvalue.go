// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package dynamicconfig

import (
	"fmt"
	"sync/atomic"

	"go.temporal.io/server/common/log/tag"
)

// GlobalCachedTypedValue holds a cached value of a GlobalTypedSetting after applying a supplied convert function.
// This type should be deleted after dynamicconfig handles conversion more efficiently.
type GlobalCachedTypedValue[T any] struct {
	ptr    atomic.Pointer[T]
	cancel func()
}

// NewGlobalCachedTypedValue returns a new [GlobalCachedTypedValue] for the given setting, applying the given convert
// function on change.
// If convert returns an error the old value will be preserved. If convert never succeeds, Get() will return the zero
// value of T.
func NewGlobalCachedTypedValue[T, I any](c *Collection, setting GlobalTypedSetting[I], convert func(I) (T, error)) *GlobalCachedTypedValue[T] {
	cachedValue := new(GlobalCachedTypedValue[T])
	initial, cancel := setting.Subscribe(c)(func(i I) {
		converted, err := convert(i)
		if err != nil {
			c.logger.Warn(fmt.Sprintf("Failed to convert %q attribute", setting.Key().String()), tag.Value(i), tag.Error(err))
			return
		}
		// Make sure to only store the value on successful conversion.
		cachedValue.ptr.Store(&converted)
	})
	cachedValue.cancel = cancel

	converted, err := convert(initial)
	if err != nil {
		c.logger.Warn(fmt.Sprintf("Failed to convert %q attribute", setting.Key().String()), tag.Value(initial), tag.Error(err))
	} else {
		// Make sure to only store the value on successful conversion.
		cachedValue.ptr.Store(&converted)
	}

	return cachedValue
}

// Get the last successfully converted cached value or the zero value if convert has never succeeded.
func (s *GlobalCachedTypedValue[T]) Get() T {
	v := s.ptr.Load()
	if v == nil {
		var t T
		return t
	}
	return *v
}

func (s *GlobalCachedTypedValue[T]) Close() error {
	s.cancel()
	return nil
}
