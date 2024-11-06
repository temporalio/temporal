package dynamicconfig

import (
	"fmt"
	"sync/atomic"

	"go.temporal.io/server/common/log/tag"
)

// GlobalCachedTypedValue holds a cached value of a GlobalTypedSetting after applying a supplied convert function.
// This type should be deleted after dynamicconfig handles conversion more efficiently.
type GlobalCachedTypedValue[T any] struct {
	ptr    *atomic.Pointer[T]
	cancel func()
}

// NewGlobalCachedTypedValue returns a new [GlobalCachedTypedValue] for the given setting, applying the given convert
// function on change.
// If convert returns an error the old value will be preserved. If convert never succeeds, Get() will return the zero
// value of T.
func NewGlobalCachedTypedValue[T, I any](c *Collection, setting GlobalTypedSetting[I], convert func(I) (T, error)) *GlobalCachedTypedValue[T] {
	ptr := &atomic.Pointer[T]{}
	initial, cancel := setting.Subscribe(c)(func(i I) {
		converted, err := convert(i)
		if err != nil {
			c.logger.Warn(fmt.Sprintf("Failed to convert %q attribute", setting.Key().String()), tag.Value(i), tag.Error(err))
			return
		}
		// Make sure to only store the value on successful conversion.
		ptr.Store(&converted)
	})
	converted, err := convert(initial)
	if err != nil {
		c.logger.Warn(fmt.Sprintf("Failed to convert %q attribute", setting.Key().String()), tag.Value(initial), tag.Error(err))
	} else {
		// Make sure to only store the value on successful conversion.
		ptr.Store(&converted)
	}

	return &GlobalCachedTypedValue[T]{
		ptr:    ptr,
		cancel: cancel,
	}
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
