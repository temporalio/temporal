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
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/tqid"
)

func TestParsePhysicalTaskQueueKey(t *testing.T) {
	a := assert.New(t)
	tt := enumspb.TASK_QUEUE_TYPE_WORKFLOW
	ns := "ns-id"
	f, err := tqid.NewTaskQueueFamily(ns, "my-basic-tq-name")
	assert.NoError(t, err)

	key, err := ParsePhysicalTaskQueueKey("my-basic-tq-name", ns, tt)
	a.NoError(err)
	a.Equal(f.TaskQueue(tt).RootPartition().Key(), key.Partition().Key())
	a.Equal("", key.Version().VersionSet())
	a.Equal("", key.Version().BuildId())
	a.Nil(key.Version().Deployment())
	a.Equal("my-basic-tq-name", key.PersistenceName())

	key, err = ParsePhysicalTaskQueueKey("/_sys/my-basic-tq-name/23", ns, tt)
	a.NoError(err)
	a.Equal(f.TaskQueue(tt).NormalPartition(23).Key(), key.Partition().Key())
	a.Equal("", key.Version().VersionSet())
	a.Equal("", key.Version().BuildId())
	a.Nil(key.Version().Deployment())
	a.Equal("/_sys/my-basic-tq-name/23", key.PersistenceName())

	key, err = ParsePhysicalTaskQueueKey("/_sys/my-basic-tq-name/verxyz:23", ns, tt)
	a.NoError(err)
	a.Equal("my-basic-tq-name", key.TaskQueueFamily().Name())
	a.Equal(f.TaskQueue(tt).NormalPartition(23).Key(), key.Partition().Key())
	a.Equal("verxyz", key.Version().VersionSet())
	a.Equal("", key.Version().BuildId())
	a.Nil(key.Version().Deployment())
	a.Equal("/_sys/my-basic-tq-name/verxyz:23", key.PersistenceName())

	buildID := "verxyz"
	encodedBuildID := base64.URLEncoding.EncodeToString([]byte(buildID))
	key, err = ParsePhysicalTaskQueueKey("/_sys/my-basic-tq-name/"+encodedBuildID+"#23", ns, tt)
	a.NoError(err)
	a.Equal("my-basic-tq-name", key.TaskQueueFamily().Name())
	a.Equal(f.TaskQueue(tt).NormalPartition(23).Key(), key.Partition().Key())
	a.Equal("", key.Version().VersionSet())
	a.Equal("verxyz", key.Version().BuildId())
	a.Nil(key.Version().Deployment())
	a.Equal("/_sys/my-basic-tq-name/"+encodedBuildID+"#23", key.PersistenceName())

	seriesName := "?deployment-ABC|ad/sf:98"
	encodedSeriesName := base64.URLEncoding.EncodeToString([]byte(seriesName))
	deployment := &deploymentpb.Deployment{
		SeriesName: seriesName,
		BuildId:    buildID,
	}
	key, err = ParsePhysicalTaskQueueKey("/_sys/my-basic-tq-name/"+encodedSeriesName+"|"+encodedBuildID+"#23", ns, tt)
	a.NoError(err)
	a.Equal("my-basic-tq-name", key.TaskQueueFamily().Name())
	a.Equal(f.TaskQueue(tt).NormalPartition(23).Key(), key.Partition().Key())
	a.Equal("", key.Version().VersionSet())
	a.Equal("", key.Version().BuildId())
	a.True(deployment.Equal(key.Version().Deployment()))
	a.Equal("/_sys/my-basic-tq-name/"+encodedSeriesName+"|"+encodedBuildID+"#23", key.PersistenceName())
}

func TestValidPersistenceNames(t *testing.T) {
	versionSet := "asdf89SD-lks_="
	buildID := "build-ABC/adsf:98"
	seriesName := "?deployment-ABC|ad/sf:98"
	v2EncodedBuildID := base64.URLEncoding.EncodeToString([]byte(buildID))
	encodedBuildID := base64.RawURLEncoding.EncodeToString([]byte(buildID))
	encodedSeriesName := base64.RawURLEncoding.EncodeToString([]byte(seriesName))
	deployment := &deploymentpb.Deployment{
		SeriesName: seriesName,
		BuildId:    buildID,
	}

	testCases := []struct {
		input      string
		baseName   string
		partition  int
		versionSet string
		buildId    string
		deployment *deploymentpb.Deployment
	}{
		{"0", "0", 0, "", "", nil},
		{"list0", "list0", 0, "", "", nil},
		{"/list0", "/list0", 0, "", "", nil},
		{"/list0/", "/list0/", 0, "", "", nil},
		{"__temporal_sys/list0", "__temporal_sys/list0", 0, "", "", nil},
		{"__temporal_sys/list0/", "__temporal_sys/list0/", 0, "", "", nil},
		{"/__temporal_sys_list0", "/__temporal_sys_list0", 0, "", "", nil},
		{"/_sys/list0/1", "list0", 1, "", "", nil},
		{"/_sys//list0//41", "/list0/", 41, "", "", nil},
		{"/_sys/list0/" + versionSet + ":1", "list0", 1, versionSet, "", nil},
		{"/_sys//list0//" + versionSet + ":41", "/list0/", 41, versionSet, "", nil},
		{"/_sys/list0/" + v2EncodedBuildID + "#1", "list0", 1, "", buildID, nil},
		{"/_sys//list0//" + v2EncodedBuildID + "#41", "/list0/", 41, "", buildID, nil},
		{"/_sys/list0/" + encodedSeriesName + "|" + encodedBuildID + "#1", "list0", 1, "", "", deployment},
		{"/_sys//list0//" + encodedSeriesName + "|" + encodedBuildID + "#41", "/list0/", 41, "", "", deployment},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			key, err := ParsePhysicalTaskQueueKey(tc.input, "", 0)
			require.NoError(t, err)
			require.Equal(t, tc.partition, key.Partition().(*tqid.NormalPartition).PartitionId())
			require.Equal(t, tc.baseName, key.TaskQueueFamily().Name())
			require.Equal(t, tc.versionSet, key.Version().VersionSet())
			require.Equal(t, tc.buildId, key.Version().BuildId())
			require.True(t, tc.deployment.Equal(key.Version().Deployment()))
			require.Equal(t, tc.input, key.PersistenceName())
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
		"/_sys/list0/|verxyz#23",   // empty deployment name
		"/_sys/list0/verxyz|#23",   // empty build id
		"/_sys/list0/c|ver$xyz#23", // invalid char in deployment name
		"/_sys/list0/ver$xyz|x#23", // invalid char in build id
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
	key := VersionSetQueueKey(p, "abc3")
	a.Equal(p, key.Partition())
	a.Equal("abc3", key.Version().VersionSet())
	a.Equal("", key.Version().BuildId())
	a.Nil(key.Version().Deployment())
}

func TestBuildIDQueueKey(t *testing.T) {
	a := assert.New(t)

	f, err := tqid.NewTaskQueueFamily("", "tq")
	assert.NoError(t, err)
	p := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(2)
	key := BuildIdQueueKey(p, "abc3")
	a.Equal(p, key.Partition())
	a.Equal("", key.Version().VersionSet())
	a.Equal("abc3", key.Version().BuildId())
	a.Nil(key.Version().Deployment())
}

func TestDeploymentQueueKey(t *testing.T) {
	a := assert.New(t)

	f, err := tqid.NewTaskQueueFamily("", "tq")
	assert.NoError(t, err)
	p := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(2)
	d := &deploymentpb.Deployment{
		SeriesName: "my_app",
		BuildId:    "abc",
	}
	key := DeploymentQueueKey(p, d)
	a.Equal(p, key.Partition())
	a.Equal("", key.Version().VersionSet())
	a.Equal("", key.Version().BuildId())
	a.True(d.Equal(key.Version().Deployment()))
}

func TestUnversionedQueueKey(t *testing.T) {
	a := assert.New(t)

	f, err := tqid.NewTaskQueueFamily("", "tq")
	assert.NoError(t, err)
	p := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(2)
	key := UnversionedQueueKey(p)
	a.Equal(p, key.Partition())
	a.Equal("", key.Version().VersionSet())
	a.Equal("", key.Version().BuildId())
	a.Nil(key.Version().Deployment())
}
