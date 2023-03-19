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

package tqname

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	a := assert.New(t)

	n, err := Parse("my-basic-tq-name")
	a.NoError(err)
	a.Equal("my-basic-tq-name", n.BaseNameString())
	a.Equal(0, n.Partition())
	a.Equal("", n.VersionSet())
	a.Equal("my-basic-tq-name", n.FullName())
	a.True(n.IsRoot())
	_, err = n.Parent(5)
	a.Equal(ErrNoParent, err)

	n, err = Parse("/_sys/my-basic-tq-name/23")
	a.NoError(err)
	a.Equal("my-basic-tq-name", n.BaseNameString())
	a.Equal(23, n.Partition())
	a.Equal("", n.VersionSet())
	a.Equal("/_sys/my-basic-tq-name/23", n.FullName())
	a.False(n.IsRoot())
	a.Equal(4, mustParent(n, 5).Partition())
	a.Equal(0, mustParent(n, 32).Partition())

	n, err = Parse("/_sys/my-basic-tq-name/verxyz:23")
	a.NoError(err)
	a.Equal("my-basic-tq-name", n.BaseNameString())
	a.Equal(23, n.Partition())
	a.Equal("verxyz", n.VersionSet())
	a.Equal("/_sys/my-basic-tq-name/verxyz:23", n.FullName())
}

func TestFromBaseName(t *testing.T) {
	a := assert.New(t)

	n, err := FromBaseName("my-basic-tq-name")
	a.NoError(err)
	a.Equal("my-basic-tq-name", n.BaseNameString())
	a.Equal(0, n.Partition())
	a.Equal("", n.VersionSet())

	_, err = FromBaseName("/_sys/my-basic-tq-name/23")
	a.Error(err)
}

func TestWithPartition(t *testing.T) {
	a := assert.New(t)

	n, err := FromBaseName("tq")
	a.NoError(err)
	n = n.WithPartition(23)
	a.Equal("tq", n.BaseNameString())
	a.Equal(23, n.Partition())
	a.Equal("/_sys/tq/23", n.FullName())
	a.False(n.IsRoot())
}

func TestWithVersionSet(t *testing.T) {
	a := assert.New(t)

	n, err := FromBaseName("tq")
	a.NoError(err)
	n = n.WithVersionSet("abc3")
	a.Equal("tq", n.BaseNameString())
	a.Equal(0, n.Partition())
	a.Equal("/_sys/tq/abc3:0", n.FullName())
}

func TestWithPartitionAndVersionSet(t *testing.T) {
	a := assert.New(t)

	n, err := FromBaseName("tq")
	a.NoError(err)
	n = n.WithPartition(11).WithVersionSet("abc3")
	a.Equal("tq", n.BaseNameString())
	a.Equal(11, n.Partition())
	a.Equal("abc3", n.VersionSet())
	a.Equal("/_sys/tq/abc3:11", n.FullName())
}

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
			tn, err := Parse(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.partition, tn.partition)
			require.Equal(t, tc.partition == 0, tn.IsRoot())
			require.Equal(t, tc.baseName, tn.baseName)
			require.Equal(t, tc.baseName, tn.BaseNameString())
			require.Equal(t, tc.input, tn.FullName())
		})
	}
}

func TestTaskQueueParentName(t *testing.T) {
	const invalid = "__invalid__"
	testCases := []struct {
		name   string
		degree int
		output string
	}{
		/* unexpected input */
		{"list0", 0, invalid},
		/* 1-ary tree */
		{"list0", 1, invalid},
		{"/_sys/list0/1", 1, "list0"},
		{"/_sys/list0/2", 1, "/_sys/list0/1"},
		/* 2-ary tree */
		{"list0", 2, invalid},
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
			tn, err := Parse(tc.name)
			require.NoError(t, err)
			parent, err := tn.Parent(tc.degree)
			if tc.output == invalid {
				require.Equal(t, ErrNoParent, err)
			} else {
				require.Equal(t, tc.output, parent.FullName())
			}
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
		"/_sys/list0/abc",
		"/_sys/list0:verxyz:23",
	}
	for _, name := range inputs {
		t.Run(name, func(t *testing.T) {
			_, err := Parse(name)
			require.Error(t, err)
		})
	}
}

func mustParent(tn Name, n int) Name {
	parent, err := tn.Parent(n)
	if err != nil {
		panic(err)
	}
	return parent
}
