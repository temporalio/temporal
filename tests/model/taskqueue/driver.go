package taskqueue

import (
	"context"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore/umpire"
)

// driver implements umpire.ModelBehavior for task queue verification. There
// is no register/delete action: queue lifecycle is implicit.
type driver struct {
	store  *umpire.EntityStore[*Entity]
	ctx    context.Context
	client workflowservice.WorkflowServiceClient
}

var _ umpire.ModelBehavior = (*driver)(nil)

func newDriver(deps Deps) *driver {
	return &driver{
		store:  deps.Store,
		ctx:    deps.Context,
		client: deps.Client,
	}
}

func (d *driver) Actions() []umpire.Action {
	return []umpire.Action{
		{
			Name:    "TaskQueueDescribe",
			Enabled: func() bool { return d.store.Len() != 0 },
			Run:     d.describe,
		},
	}
}

func (d *driver) describe(t *umpire.T) {
	tq := d.store.Pick(t, "taskQueueAny", anyTaskQueue)

	_, err := d.client.DescribeTaskQueue(d.ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace: tq.Namespace,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: tq.Name,
			Kind: tq.Kind,
		},
		TaskQueueType: tq.Type,
	})
	t.NoError(err)
}

func (d *driver) CheckInvariant(t *umpire.T) {
	for _, tq := range d.store.All() {
		_, err := d.client.DescribeTaskQueue(d.ctx, &workflowservice.DescribeTaskQueueRequest{
			Namespace: tq.Namespace,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tq.Name,
				Kind: tq.Kind,
			},
			TaskQueueType: tq.Type,
		})
		t.NoError(err, "invariant: task queue %s/%s describe failed", tq.Namespace, tq.Name)
	}
}

func (d *driver) Cleanup(t *umpire.T) {
	// Task queues have no explicit deletion. Cluster teardown reclaims them.
	_ = t
}

func anyTaskQueue(*Entity) bool { return true }
