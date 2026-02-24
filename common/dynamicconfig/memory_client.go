package dynamicconfig

import (
	"sync"
	"sync/atomic"
)

type (
	MemoryClient struct {
		lock      sync.RWMutex
		overrides []kvpair

		NotifyingClientImpl
	}

	kvpair struct {
		valid     bool
		mergeable bool
		key       Key
		value     any
	}
)

// NewMemoryClient - returns a memory based dynamic config client
func NewMemoryClient() *MemoryClient {
	return &MemoryClient{NotifyingClientImpl: NewNotifyingClientImpl()}
}

func (d *MemoryClient) GetValue(key Key) []ConstrainedValue {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.getValueLocked(key)
}

func (d *MemoryClient) getValueLocked(key Key) []ConstrainedValue {
	var result []ConstrainedValue
	for i := len(d.overrides) - 1; i >= 0; i-- {
		if d.overrides[i].valid && d.overrides[i].key == key {
			v := d.overrides[i].value
			if cvs, ok := v.([]ConstrainedValue); ok {
				result = append(result, cvs...)
			} else {
				result = append(result, ConstrainedValue{Value: v})
			}
			if !d.overrides[i].mergeable {
				// Non-mergeable: take this value and stop.
				return result
			}
		}
	}
	return result
}

func (d *MemoryClient) OverrideSetting(setting GenericSetting, value any) (cleanup func()) {
	return d.OverrideValue(setting.Key(), value)
}

func (d *MemoryClient) OverrideValue(key Key, value any) (cleanup func()) {
	return d.overrideValue(key, value, false)
}

// PartialOverrideSetting is like OverrideSetting but marks the override as mergeable.
// Mergeable overrides accumulate with other mergeable overrides for the same key,
// rather than shadowing them. This is used for namespace-constrained overrides on
// shared test clusters, where parallel tests need their overrides to coexist.
func (d *MemoryClient) PartialOverrideSetting(setting GenericSetting, value any) (cleanup func()) {
	return d.PartialOverrideValue(setting.Key(), value)
}

// PartialOverrideValue is like OverrideValue but marks the override as mergeable.
func (d *MemoryClient) PartialOverrideValue(key Key, value any) (cleanup func()) {
	return d.overrideValue(key, value, true)
}

func (d *MemoryClient) overrideValue(key Key, value any, mergeable bool) (cleanup func()) {
	d.lock.Lock()

	var idx atomic.Int64
	idx.Store(int64(len(d.overrides)))

	d.overrides = append(d.overrides, kvpair{valid: true, mergeable: mergeable, key: key, value: value})

	newValue := d.getValueLocked(key)
	changed := map[Key][]ConstrainedValue{key: newValue}

	d.lock.Unlock()

	// do not hold lock while notifying subscriptions
	d.PublishUpdates(changed)

	return func() {
		// only do this once
		if removeIdx := int(idx.Swap(-1)); removeIdx >= 0 {
			d.remove(removeIdx)
		}
	}
}

func (d *MemoryClient) remove(idx int) {
	d.lock.Lock()

	key := d.overrides[idx].key
	// mark this pair deleted
	d.overrides[idx] = kvpair{}

	// pop all deleted pairs
	for l := len(d.overrides); l > 0 && !d.overrides[l-1].valid; l = len(d.overrides) {
		d.overrides = d.overrides[:l-1]
	}

	newValue := d.getValueLocked(key)
	changed := map[Key][]ConstrainedValue{key: newValue}

	d.lock.Unlock()

	// do not hold lock while notifying subscriptions
	d.PublishUpdates(changed)
}
