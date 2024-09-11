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

package statsd

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetricNameWithTags(t *testing.T) {
	r := temporalTallyStatsdReporter{
		tallystatsd: nil,
	}
	tags := map[string]string{
		"tag1": "123",
		"tag2": "456",
		"tag3": "789",
	}
	name := "test-metric-name1"

	assert.Equal(t, "test-metric-name1.tag1.123.tag2.456.tag3.789", r.metricNameWithTags(name, tags))
}

func TestMetricNameWithTagsStability(t *testing.T) {
	r := temporalTallyStatsdReporter{
		tallystatsd: nil,
	}
	tags := map[string]string{
		"tag1": "123",
		"tag2": "456",
		"tag3": "789",
	}
	name := "test-metric-name2"

	//test the order is stable
	for i := 1; i <= 16; i++ {
		assert.Equal(t, r.metricNameWithTags(name, tags), r.metricNameWithTags(name, tags))
	}
}

func TestMetricNameWithSeparatedTags(t *testing.T) {
	testCases := []string{
		"",
		",",
		"__",
		".__",
	}
	for i, tc := range testCases {
		sep := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r := temporalTallyStatsdReporter{
				tagSeparator: sep,
			}
			tags := map[string]string{
				"tag1": "123",
				"tag2": "456",
				"tag3": "789",
			}
			name := "test-metric-name3"

			newName := r.metricNameWithTags(name, tags)

			if sep == "" {
				// Tags should be embedded.
				assert.Equal(t, newName, "test-metric-name3.tag1.123.tag2.456.tag3.789")
			} else {
				// Tags will be appended with the separator.
				assert.True(t, strings.HasPrefix(newName, name))

				ss := strings.Split(newName, sep)
				assert.Len(t, ss, 1+len(tags))
				assert.Equal(t, ss[0], name)
				assert.Contains(t, ss, "tag1=123")
				assert.Contains(t, ss, "tag2=456")
				assert.Contains(t, ss, "tag3=789")
			}
		})
	}

}
