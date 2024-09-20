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
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/tqid"
)

func TestParsePhysicalTaskQueueKey(t *testing.T) {
	a := assert.New(t)
	tt := enumspb.TASK_QUEUE_TYPE_WORKFLOW
	ns := "ns-id"
	f, err := tqid.NewTaskQueueFamily(ns, "my-basic-tq-name")
	assert.NoError(t, err)

	dbq, err := ParsePhysicalTaskQueueKey("my-basic-tq-name", ns, tt)
	a.NoError(err)
	a.Equal(f.TaskQueue(tt).RootPartition().Key(), dbq.Partition().Key())
	a.Equal("", dbq.VersionSet())
	a.Equal("", dbq.BuildId())
	a.Equal("my-basic-tq-name", dbq.PersistenceName())

	dbq, err = ParsePhysicalTaskQueueKey("/_sys/my-basic-tq-name/23", ns, tt)
	a.NoError(err)
	a.Equal(f.TaskQueue(tt).NormalPartition(23).Key(), dbq.Partition().Key())
	a.Equal("", dbq.VersionSet())
	a.Equal("", dbq.BuildId())
	a.Equal("/_sys/my-basic-tq-name/23", dbq.PersistenceName())

	dbq, err = ParsePhysicalTaskQueueKey("/_sys/my-basic-tq-name/verxyz:23", ns, tt)
	a.NoError(err)
	a.Equal("my-basic-tq-name", dbq.TaskQueueFamily().Name())
	a.Equal(f.TaskQueue(tt).NormalPartition(23).Key(), dbq.Partition().Key())
	a.Equal("verxyz", dbq.VersionSet())
	a.Equal("", dbq.BuildId())
	a.Equal("/_sys/my-basic-tq-name/verxyz:23", dbq.PersistenceName())

	buildID := "verxyz"
	encodedBuildID := base64.URLEncoding.EncodeToString([]byte(buildID))
	dbq, err = ParsePhysicalTaskQueueKey("/_sys/my-basic-tq-name/"+encodedBuildID+"#23", ns, tt)
	a.NoError(err)
	a.Equal("my-basic-tq-name", dbq.TaskQueueFamily().Name())
	a.Equal(f.TaskQueue(tt).NormalPartition(23).Key(), dbq.Partition().Key())
	a.Equal("", dbq.VersionSet())
	a.Equal("verxyz", dbq.BuildId())
	a.Equal("/_sys/my-basic-tq-name/"+encodedBuildID+"#23", dbq.PersistenceName())
}

func TestValidPersistenceNames(t *testing.T) {
	versionSet := "asdf89SD-lks_="
	buildID := "build-ABC/adsf:98"
	encodedBuildID := base64.URLEncoding.EncodeToString([]byte(buildID))

	testCases := []struct {
		input      string
		baseName   string
		partition  int
		versionSet string
		buildId    string
	}{
		{"0", "0", 0, "", ""},
		{"list0", "list0", 0, "", ""},
		{"/list0", "/list0", 0, "", ""},
		{"/list0/", "/list0/", 0, "", ""},
		{"__temporal_sys/list0", "__temporal_sys/list0", 0, "", ""},
		{"__temporal_sys/list0/", "__temporal_sys/list0/", 0, "", ""},
		{"/__temporal_sys_list0", "/__temporal_sys_list0", 0, "", ""},
		{"/_sys/list0/1", "list0", 1, "", ""},
		{"/_sys//list0//41", "/list0/", 41, "", ""},
		{"/_sys/list0/" + versionSet + ":1", "list0", 1, versionSet, ""},
		{"/_sys//list0//" + versionSet + ":41", "/list0/", 41, versionSet, ""},
		{"/_sys/list0/" + encodedBuildID + "#1", "list0", 1, "", buildID},
		{"/_sys//list0//" + encodedBuildID + "#41", "/list0/", 41, "", buildID},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			dbq, err := ParsePhysicalTaskQueueKey(tc.input, "", 0)
			require.NoError(t, err)
			require.Equal(t, tc.partition, dbq.Partition().(*tqid.NormalPartition).PartitionId())
			require.Equal(t, tc.baseName, dbq.TaskQueueFamily().Name())
			require.Equal(t, tc.versionSet, dbq.VersionSet())
			require.Equal(t, tc.buildId, dbq.BuildId())
			require.Equal(t, tc.input, dbq.PersistenceName())
		})
	}
}

func TestInvalidPersistenceNames(t *testing.T) {
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
		"/_sys/list0/ve$xyz#23",
		"/_sys/list0:verxyz#23",
	}
	for _, name := range inputs {
		t.Run(name, func(t *testing.T) {
			_, err := ParsePhysicalTaskQueueKey(name, "", 0)
			require.Error(t, err)
		})
	}
}

func TestVersionSetQueueKey(t *testing.T) {
	a := assert.New(t)

	f, err := tqid.NewTaskQueueFamily("", "tq")
	assert.NoError(t, err)
	p := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(2)
	dbq := VersionSetQueueKey(p, "abc3")
	a.Equal(p, dbq.Partition())
	a.Equal("abc3", dbq.VersionSet())
	a.Equal("", dbq.BuildId())
}

func TestBuildIDQueueKey(t *testing.T) {
	a := assert.New(t)

	f, err := tqid.NewTaskQueueFamily("", "tq")
	assert.NoError(t, err)
	p := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(2)
	dbq := BuildIdQueueKey(p, "abc3")
	a.Equal(p, dbq.Partition())
	a.Equal("", dbq.VersionSet())
	a.Equal("abc3", dbq.BuildId())
}

func TestUnversionedQueueKey(t *testing.T) {
	a := assert.New(t)

	f, err := tqid.NewTaskQueueFamily("", "tq")
	assert.NoError(t, err)
	p := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(2)
	dbq := UnversionedQueueKey(p)
	a.Equal(p, dbq.Partition())
	a.Equal("", dbq.VersionSet())
	a.Equal("", dbq.BuildId())
}
