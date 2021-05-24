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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination interface_mock.go

package dynamicconfig

import (
	"time"
)

// Client allows fetching values from a dynamic configuration system NOTE: This does not have async
// options right now. In the interest of keeping it minimal, we can add when requirement arises.
type Client interface {
	GetValue(name Key, defaultValue interface{}) (interface{}, error)
	GetValueWithFilters(name Key, filters map[Filter]interface{}, defaultValue interface{}) (interface{}, error)

	GetIntValue(name Key, filters map[Filter]interface{}, defaultValue int) (int, error)
	GetFloatValue(name Key, filters map[Filter]interface{}, defaultValue float64) (float64, error)
	GetBoolValue(name Key, filters map[Filter]interface{}, defaultValue bool) (bool, error)
	GetStringValue(name Key, filters map[Filter]interface{}, defaultValue string) (string, error)
	GetMapValue(
		name Key, filters map[Filter]interface{}, defaultValue map[string]interface{},
	) (map[string]interface{}, error)
	GetDurationValue(
		name Key, filters map[Filter]interface{}, defaultValue time.Duration,
	) (time.Duration, error)
}
