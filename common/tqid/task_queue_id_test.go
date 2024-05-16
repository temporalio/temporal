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

package tqid

import (
	"errors"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
)

func TestFromProtoPartition_Sticky(t *testing.T) {
	a := assert.New(t)

	nsid := "my-namespace"
	stickyName := "a8sdkf5ks"
	normalName := "very-normal"
	taskType := enumspb.TASK_QUEUE_TYPE_WORKFLOW
	kind := enumspb.TASK_QUEUE_KIND_STICKY
	proto := &taskqueuepb.TaskQueue{
		Name:       stickyName,
		Kind:       kind,
		NormalName: normalName,
	}

	p, err := PartitionFromProto(proto, nsid, taskType)
	a.NoError(err)
	a.Equal(nsid, p.NamespaceId().String())
	a.Equal(taskType, p.TaskType())
	a.Equal(kind, p.Kind())
	a.Equal(normalName, p.TaskQueue().Name())
	a.Equal(stickyName, p.(*StickyPartition).StickyName())
	a.Equal(stickyName, p.RpcName())
	a.False(p.IsRoot())
	a.Equal(PartitionKey{nsid, stickyName, 0, taskType}, p.Key())

	// should be able to parse without normal name, old clients may not send normal name.
	proto.NormalName = ""
	p, err = PartitionFromProto(proto, nsid, taskType)
	a.NoError(err)
	a.Equal(nsid, p.NamespaceId().String())
	a.Equal(taskType, p.TaskType())
	a.Equal(kind, p.Kind())
	a.Equal("", p.TaskQueue().Name())
	a.Equal(stickyName, p.(*StickyPartition).StickyName())
	a.Equal(stickyName, p.RpcName())
	a.False(p.IsRoot())
	a.Equal(PartitionKey{nsid, stickyName, 0, taskType}, p.Key())

	proto.Name = "/_sys/my-basic-tq-name/23"
	_, err = PartitionFromProto(proto, nsid, taskType)
	// sticky queue cannot have non-zero prtn
	a.True(errors.Is(err, ErrNonZeroSticky))
}

func TestFromProtoPartition_Normal(t *testing.T) {
	a := assert.New(t)

	nsid := "my-namespace"
	tqname := "my-basic-tq-name"
	taskType := enumspb.TASK_QUEUE_TYPE_WORKFLOW
	kind := enumspb.TASK_QUEUE_KIND_NORMAL
	proto := &taskqueuepb.TaskQueue{
		Name: tqname,
		Kind: kind,
	}

	p, err := PartitionFromProto(proto, nsid, taskType)
	a.NoError(err)
	a.Equal(nsid, p.NamespaceId().String())
	a.Equal(taskType, p.TaskType())
	a.Equal(kind, p.Kind())
	a.Equal(tqname, p.TaskQueue().Name())
	a.Equal(tqname, p.RpcName())
	a.True(p.IsRoot())
	a.Equal(PartitionKey{nsid, tqname, 0, taskType}, p.Key())

	proto.NormalName = "something"
	_, err = PartitionFromProto(proto, nsid, taskType)
	// normal queue cannot have normal name
	a.Error(err)

	proto.Name = "/_sys/my-basic-tq-name/23"
	proto.NormalName = ""
	p, err = PartitionFromProto(proto, nsid, taskType)
	a.NoError(err)
	a.Equal(nsid, p.NamespaceId().String())
	a.Equal(tqname, p.TaskQueue().Name())
	a.Equal(taskType, p.TaskType())
	a.Equal(kind, p.Kind())
	a.Equal(23, p.(*NormalPartition).PartitionId())
	a.Equal("/_sys/my-basic-tq-name/23", p.RpcName())
	a.False(p.IsRoot())
	a.Equal(PartitionKey{nsid, tqname, 23, taskType}, p.Key())
	a.Equal(4, mustParent(p, 5).PartitionId())
	a.Equal(0, mustParent(p, 32).PartitionId())

	proto.Name = "/_sys/my-basic-tq-name/verxyz:23"
	_, err = PartitionFromProto(proto, nsid, taskType)
	a.Error(err)

	proto.Name = "/_sys/my-basic-tq-name/verxyz#23"
	_, err = PartitionFromProto(proto, nsid, taskType)
	a.Error(err)
}

func TestFromBaseName(t *testing.T) {
	a := assert.New(t)

	f, err := NewTaskQueueFamily("", "my-basic-tq-name")
	a.NoError(err)
	a.Equal("my-basic-tq-name", f.Name())

	_, err = NewTaskQueueFamily("", "/_sys/my-basic-tq-name/23")
	a.Error(err)
}

func TestNormalPartition(t *testing.T) {
	a := assert.New(t)

	f, err := NewTaskQueueFamily("", "tq")
	a.NoError(err)
	p := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(23)
	a.Equal("tq", p.TaskQueue().Name())
	a.Equal(23, p.PartitionId())
	a.Equal("/_sys/tq/23", p.RpcName())
	a.False(p.IsRoot())
}

func TestValidRpcNames(t *testing.T) {
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
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			p := mustParseNormalPartition(t, tc.input, 0)
			require.Equal(t, tc.partition, p.PartitionId())
			require.Equal(t, tc.partition == 0, p.IsRoot())
			require.Equal(t, tc.baseName, p.TaskQueue().Name())
			require.Equal(t, tc.input, p.RpcName())
		})
	}
}

func TestParentName(t *testing.T) {
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
			p := mustParseNormalPartition(t, tc.name, enumspb.TaskQueueType(rand.Intn(3)))
			parent, err := p.ParentPartition(tc.degree)
			if tc.output == invalid {
				require.Equal(t, ErrNoParent, err)
			} else {
				require.Equal(t, tc.output, parent.RpcName())
				require.Equal(t, p.TaskType(), parent.TaskType())
			}
		})
	}
}

func TestInvalidRpcNames(t *testing.T) {
	inputs := []string{
		"/_sys/",
		"/_sys/0",
		"/_sys//1",
		"/_sys//0",
		"/_sys/list0",
		"/_sys/list0/0",
		"/_sys/list0/-1",
		"/_sys/list0/abc",
		"/_sys//_sys/sys/0/41",
		"/_sys/list0:verxyz:23",
		"/_sys/list0/verxyz:23",
		"/_sys/list0/verxyz#23",
	}
	for _, name := range inputs {
		t.Run(name, func(t *testing.T) {
			_, err := PartitionFromProto(&taskqueuepb.TaskQueue{Name: name}, "", 0)
			require.Error(t, err)
		})
	}
}

func mustParseNormalPartition(t *testing.T, rpcName string, taskType enumspb.TaskQueueType) *NormalPartition {
	p, err := PartitionFromProto(&taskqueuepb.TaskQueue{Name: rpcName}, "", taskType)
	require.NoError(t, err)
	res, ok := p.(*NormalPartition)
	require.True(t, ok)
	return res
}

func mustParent(p Partition, n int) *NormalPartition {
	normalPrtn, ok := p.(*NormalPartition)
	if !ok {
		panic("not a normal partition")
	}
	parent, err := normalPrtn.ParentPartition(n)
	if err != nil {
		panic(err)
	}
	return parent
}
