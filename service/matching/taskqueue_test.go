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

package matching

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidTaskQueueNames(t *testing.T) {
	testCases := []struct {
		input     string
		baseName  string
		partition int
	}{
		{"0", "0", 0},
		{"list0", "list0", 0},
		{"/list0", "/list0", 0},
		{"/list0/", "/list0/", 0},
		{"__temporal_sys/list0", "__temporal_sys/list0", 0},
		{"__temporal_sys/list0/", "__temporal_sys/list0/", 0},
		{"/__temporal_sys_list0", "/__temporal_sys_list0", 0},
		{"/_sys/list0/1", "list0", 1},
		{"/_sys//list0//41", "/list0/", 41},
		{"/_sys//_sys/sys/0/41", "/_sys/sys/0", 41},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			tn, err := newTaskQueueName(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.partition, tn.partition)
			require.Equal(t, tc.partition == 0, tn.IsRoot())
			require.Equal(t, tc.baseName, tn.baseName)
			require.Equal(t, tc.baseName, tn.GetRoot())
			require.Equal(t, tc.input, tn.name)
		})
	}
}

func TestTaskQueueParentName(t *testing.T) {
	testCases := []struct {
		name   string
		degree int
		output string
	}{
		/* unexpected input */
		{"list0", 0, ""},
		/* 1-ary tree */
		{"list0", 1, ""},
		{"/_sys/list0/1", 1, "list0"},
		{"/_sys/list0/2", 1, "/_sys/list0/1"},
		/* 2-ary tree */
		{"list0", 2, ""},
		{"/_sys/list0/1", 2, "list0"},
		{"/_sys/list0/2", 2, "list0"},
		{"/_sys/list0/3", 2, "/_sys/list0/1"},
		{"/_sys/list0/4", 2, "/_sys/list0/1"},
		{"/_sys/list0/5", 2, "/_sys/list0/2"},
		{"/_sys/list0/6", 2, "/_sys/list0/2"},
		/* 3-ary tree */
		{"/_sys/list0/1", 3, "list0"},
		{"/_sys/list0/2", 3, "list0"},
		{"/_sys/list0/3", 3, "list0"},
		{"/_sys/list0/4", 3, "/_sys/list0/1"},
		{"/_sys/list0/5", 3, "/_sys/list0/1"},
		{"/_sys/list0/6", 3, "/_sys/list0/1"},
		{"/_sys/list0/7", 3, "/_sys/list0/2"},
		{"/_sys/list0/10", 3, "/_sys/list0/3"},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"#"+strconv.Itoa(tc.degree), func(t *testing.T) {
			tn, err := newTaskQueueName(tc.name)
			require.NoError(t, err)
			require.Equal(t, tc.output, tn.Parent(tc.degree))
		})
	}
}

func TestInvalidTaskqueueNames(t *testing.T) {
	inputs := []string{
		"/_sys/",
		"/_sys/0",
		"/_sys//1",
		"/_sys//0",
		"/_sys/list0",
		"/_sys/list0/0",
		"/_sys/list0/-1",
	}
	for _, name := range inputs {
		t.Run(name, func(t *testing.T) {
			_, err := newTaskQueueName(name)
			require.Error(t, err)
		})
	}
}
