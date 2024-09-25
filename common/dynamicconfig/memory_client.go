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

package dynamicconfig

import (
	"sync"
	"sync/atomic"
)

type (
	MemoryClient struct {
		lock      sync.RWMutex
		overrides []kvpair
	}

	kvpair struct {
		valid bool
		key   Key
		value any
	}
)

// NewMemoryClient - returns a memory based dynamic config client
func NewMemoryClient() *MemoryClient {
	return &MemoryClient{}
}

func (d *MemoryClient) GetValue(key Key) []ConstrainedValue {
	d.lock.RLock()
	defer d.lock.RUnlock()

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

func (d *MemoryClient) OverrideSetting(setting GenericSetting, value any) (cleanup func()) {
	return d.OverrideValue(setting.Key(), value)
}

func (d *MemoryClient) OverrideValue(key Key, value any) (cleanup func()) {
	d.lock.Lock()
	defer d.lock.Unlock()

	var idx atomic.Int64
	idx.Store(int64(len(d.overrides)))

	d.overrides = append(d.overrides, kvpair{valid: true, key: key, value: value})

	return func() {
		// only do this once
		if removeIdx := int(idx.Swap(-1)); removeIdx >= 0 {
			d.remove(removeIdx)
		}
	}
}

func (d *MemoryClient) remove(idx int) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// mark this pair deleted
	d.overrides[idx] = kvpair{}

	// pop all deleted pairs
	for l := len(d.overrides); l > 0 && !d.overrides[l-1].valid; l = len(d.overrides) {
		d.overrides = d.overrides[:l-1]
	}
}
