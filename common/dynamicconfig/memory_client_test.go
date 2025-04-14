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

package dynamicconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestMemoryClient(t *testing.T) {
	c := dynamicconfig.NewMemoryClient()
	k := dynamicconfig.Key("key")

	// plain override
	assert.Nil(t, c.GetValue(k))
	remove := c.OverrideValue(k, 123)
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 123}}, c.GetValue(k))
	remove()
	assert.Nil(t, c.GetValue(k))

	// two levels, pop in correct order
	remove1 := c.OverrideValue(k, 123)
	remove2 := c.OverrideValue(k, 456)
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 456}}, c.GetValue(k))
	remove2()
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 123}}, c.GetValue(k))
	remove1()
	assert.Nil(t, c.GetValue(k))

	// three levels, pop in wrong order
	remove1 = c.OverrideValue(k, 123)
	remove2 = c.OverrideValue(k, 456)
	remove3 := c.OverrideValue(k, 789)
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 789}}, c.GetValue(k))
	remove2()
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 789}}, c.GetValue(k))
	remove3()
	assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 123}}, c.GetValue(k))
	remove1()
	remove3() // no-op
	remove2() // no-op
	assert.Nil(t, c.GetValue(k))
}

func TestMemoryClientSubscriptions(t *testing.T) {
	c := dynamicconfig.NewMemoryClient()
	k := dynamicconfig.Key("key")

	calls := 0

	c.Subscribe(func(changed map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue) {
		calls++
		assert.Contains(t, changed, k)
		switch calls {
		case 1:
			assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 123}}, changed[k])
		case 2:
			assert.Nil(t, changed[k])
		case 3:
			assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 456}}, changed[k])
		case 4:
			assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 789}}, changed[k])
		case 5:
			assert.Equal(t, []dynamicconfig.ConstrainedValue{{Value: 456}}, changed[k])
		case 6:
			assert.Nil(t, changed[k])
		}
	})

	remove := c.OverrideValue(k, 123)
	assert.Equal(t, 1, calls)
	remove()
	assert.Equal(t, 2, calls)

	remove1 := c.OverrideValue(k, 456)
	assert.Equal(t, 3, calls)
	remove2 := c.OverrideValue(k, 789)
	assert.Equal(t, 4, calls)
	remove2()
	assert.Equal(t, 5, calls)
	remove1()
	assert.Equal(t, 6, calls)
}
