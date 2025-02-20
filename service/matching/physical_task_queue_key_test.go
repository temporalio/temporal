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
	"testing"

	"github.com/stretchr/testify/assert"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/tqid"
)

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
