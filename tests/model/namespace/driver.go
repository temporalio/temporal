package namespace

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore/umpire"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultMaxNamespaces      = 2
	namespaceRetention        = 24 * time.Hour
	namespaceCachePollTimeout = 10 * time.Second
)

// driver implements umpire.ModelBehavior for namespace lifecycle.
type driver struct {
	store         *umpire.EntityStore[*Entity]
	ctx           context.Context
	client        workflowservice.WorkflowServiceClient
	prefix        string
	maxNamespaces int
	nextSeq       int
}

var _ umpire.ModelBehavior = (*driver)(nil)

func newDriver(deps Deps) *driver {
	max := deps.MaxNamespaces
	if max == 0 {
		max = defaultMaxNamespaces
	}
	return &driver{
		store:         deps.Store,
		ctx:           deps.Context,
		client:        deps.Client,
		prefix:        deps.Prefix,
		maxNamespaces: max,
	}
}

func (d *driver) Actions() []umpire.Action {
	return []umpire.Action{
		{
			Name:    "NamespaceRegister",
			Enabled: func() bool { return d.nextSeq < d.maxNamespaces },
			Run:     d.register,
		},
		{
			Name:    "NamespaceDescribe",
			Enabled: func() bool { return d.store.Len() != 0 },
			Run:     d.describe,
		},
	}
}

func (d *driver) register(t *umpire.T) {
	name := fmt.Sprintf("prop-ns-%s-%d", d.prefix, d.nextSeq)
	d.nextSeq++

	_, err := d.client.RegisterNamespace(d.ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        name,
		Description:                      "umpire prop-test namespace",
		WorkflowExecutionRetentionPeriod: durationpb.New(namespaceRetention),
	})
	t.NoError(err)

	t.Eventually(func() bool {
		_, e := d.client.DescribeNamespace(d.ctx, &workflowservice.DescribeNamespaceRequest{Namespace: name})
		return e == nil
	}, namespaceCachePollTimeout, 100*time.Millisecond, "namespace %q not visible in cache", name)
	// World mutation happens server-side via the observer.
}

func (d *driver) describe(t *umpire.T) {
	ns := d.store.Pick(t, "namespaceAny", anyNamespace)

	resp, err := d.client.DescribeNamespace(d.ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns.Name,
	})
	if isDeletedErr(err, ns) {
		return
	}
	t.NoError(err)
	t.Equal(ToProto(ns.Status), resp.GetNamespaceInfo().GetState(),
		"namespace %s: expected %v, got %v", ns.Name, ToProto(ns.Status), resp.GetNamespaceInfo().GetState())
}

func (d *driver) CheckInvariant(t *umpire.T) {
	for _, ns := range d.store.All() {
		resp, err := d.client.DescribeNamespace(d.ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: ns.Name,
		})
		if isDeletedErr(err, ns) {
			continue
		}
		t.NoError(err)
		t.Equal(ToProto(ns.Status), resp.GetNamespaceInfo().GetState(),
			"invariant: namespace %s state mismatch", ns.Name)
	}
}

func (d *driver) Cleanup(t *umpire.T) {
	// Public WorkflowService doesn't expose namespace deletion. The dedicated
	// test cluster is torn down after this test, so leaked namespaces are
	// reclaimed implicitly.
	_ = t
}

func anyNamespace(*Entity) bool { return true }

func isDeletedErr(err error, ns *Entity) bool {
	if ns.Status != StatusDeleted {
		return false
	}
	var notFoundErr *serviceerror.NamespaceNotFound
	return errors.As(err, &notFoundErr)
}
