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
)

type MemoryClient struct {
	lock      sync.RWMutex
	overrides map[Key]any
}

func (d *MemoryClient) GetValue(name Key) []ConstrainedValue {
	d.lock.RLock()
	defer d.lock.RUnlock()
	if v, ok := d.overrides[name]; ok {
		if value, ok := v.([]ConstrainedValue); ok {
			return value
		}
		return []ConstrainedValue{{Value: v}}
	}
	return nil
}

func (d *MemoryClient) OverrideValue(setting GenericSetting, value any) {
	d.OverrideValueByKey(setting.Key(), value)
}

func (d *MemoryClient) OverrideValueByKey(name Key, value any) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.overrides[name] = value
}

func (d *MemoryClient) RemoveOverride(setting GenericSetting) {
	d.RemoveOverrideByKey(setting.Key())
}

func (d *MemoryClient) RemoveOverrideByKey(name Key) {
	d.lock.Lock()
	defer d.lock.Unlock()
	delete(d.overrides, name)
}

// NewMemoryClient - returns a memory based dynamic config client
func NewMemoryClient() *MemoryClient {
	return &MemoryClient{
		overrides: make(map[Key]any),
	}
}
