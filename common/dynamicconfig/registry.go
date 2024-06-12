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
	"fmt"
	"strings"
	"sync/atomic"
)

type (
	registry struct {
		settings map[string]GenericSetting
		queried  atomic.Bool
	}
)

var (
	globalRegistry registry
)

func register(s GenericSetting) {
	if globalRegistry.queried.Load() {
		panic("dynamicconfig.New*Setting must only be called from static initializers")
	}
	if globalRegistry.settings == nil {
		globalRegistry.settings = make(map[string]GenericSetting)
	}
	keyStr := strings.ToLower(s.Key().String())
	if globalRegistry.settings[keyStr] != nil {
		panic(fmt.Sprintf("duplicate registration of dynamic config key: %q", keyStr))
	}
	globalRegistry.settings[keyStr] = s
}

func queryRegistry(k Key) GenericSetting {
	if !globalRegistry.queried.Load() {
		globalRegistry.queried.Store(true)
	}
	return globalRegistry.settings[strings.ToLower(k.String())]
}

// For testing only; do not call from regular code!
func ResetRegistryForTest() {
	globalRegistry.settings = nil
	globalRegistry.queried.Store(false)
}
