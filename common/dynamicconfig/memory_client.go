package dynamicconfig

import (
	"sync"
	"sync/atomic"

	expmaps "golang.org/x/exp/maps"
)

type (
	MemoryClient struct {
		lock      sync.RWMutex
		overrides []kvpair

		subscriptionIdx int
		subscriptions   map[int]ClientUpdateFunc
	}

	kvpair struct {
		valid bool
		key   Key
		value any
	}
)

// NewMemoryClient - returns a memory based dynamic config client
func NewMemoryClient() *MemoryClient {
	return &MemoryClient{subscriptions: make(map[int]ClientUpdateFunc)}
}

func (d *MemoryClient) GetValue(key Key) []ConstrainedValue {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.getValueLocked(key)
}

func (d *MemoryClient) getValueLocked(key Key) []ConstrainedValue {
	for i := len(d.overrides) - 1; i >= 0; i-- {
		if d.overrides[i].valid && d.overrides[i].key == key {
			v := d.overrides[i].value
			if value, ok := v.([]ConstrainedValue); ok {
				return value
			}
			return []ConstrainedValue{{Value: v}}
		}
	}
	return nil
}

func (d *MemoryClient) Subscribe(f ClientUpdateFunc) (cancel func()) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.subscriptionIdx++
	id := d.subscriptionIdx
	d.subscriptions[id] = f

	return func() {
		d.lock.Lock()
		defer d.lock.Unlock()
		delete(d.subscriptions, id)
	}
}

func (d *MemoryClient) OverrideSetting(setting GenericSetting, value any) (cleanup func()) {
	return d.OverrideValue(setting.Key(), value)
}

func (d *MemoryClient) OverrideValue(key Key, value any) (cleanup func()) {
	d.lock.Lock()

	var idx atomic.Int64
	idx.Store(int64(len(d.overrides)))

	d.overrides = append(d.overrides, kvpair{valid: true, key: key, value: value})

	newValue := d.getValueLocked(key)
	changed := map[Key][]ConstrainedValue{key: newValue}
	subscriptions := expmaps.Values(d.subscriptions)

	d.lock.Unlock()

	// do not hold lock while notifying subscriptions
	for _, update := range subscriptions {
		update(changed)
	}

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
	subscriptions := expmaps.Values(d.subscriptions)

	d.lock.Unlock()

	// do not hold lock while notifying subscriptions
	for _, update := range subscriptions {
		update(changed)
	}
}
