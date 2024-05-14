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

package common

import (
	"encoding/json"

	"go.temporal.io/server/common/dynamicconfig"
)

// NewActivityWorkerConcurrencyConfig constructs an ActivityWorkerLimitsConfig from the map of values identified
// by key in the dynamic collection. Any errors are ignored and 0 values will be returned instead.
func NewActivityWorkerConcurrencyConfig(
	dc *dynamicconfig.Collection,
	setting dynamicconfig.GlobalMapSetting,
) ActivityWorkerLimitsConfig {
	dcOptions := setting.Get(dc)()
	var config ActivityWorkerLimitsConfig
	b, err := json.Marshal(dcOptions)
	if err != nil {
		return config
	}
	_ = json.Unmarshal(b, &config) // ignore errors, just use the zero value anyway
	return config
}
