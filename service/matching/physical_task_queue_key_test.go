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
