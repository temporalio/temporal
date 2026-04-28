package nexusendpoint

import (
	"context"
	"errors"
	"fmt"

	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/tests/testcore/umpire"
)

const defaultMaxEndpoints = 2

// driver implements umpire.ModelBehavior for the nexus endpoint lifecycle.
type driver struct {
	store        *umpire.EntityStore[*Entity]
	ctx          context.Context
	operator     operatorservice.OperatorServiceClient
	namespace    string
	prefix       string
	maxEndpoints int
	nextSeq      int
}

var _ umpire.ModelBehavior = (*driver)(nil)

func newDriver(deps Deps) *driver {
	max := deps.MaxEndpoints
	if max == 0 {
		max = defaultMaxEndpoints
	}
	return &driver{
		store:        deps.Store,
		ctx:          deps.Context,
		operator:     deps.Operator,
		namespace:    deps.Namespace,
		prefix:       deps.Prefix,
		maxEndpoints: max,
	}
}

func (d *driver) Actions() []umpire.Action {
	return []umpire.Action{
		{
			Name:    "EndpointCreate",
			Enabled: func() bool { return d.nextSeq < d.maxEndpoints },
			Run:     d.create,
		},
		{
			Name:    "EndpointDescribe",
			Enabled: func() bool { return d.store.Len() != 0 },
			Run:     d.describe,
		},
		{
			Name:    "EndpointDelete",
			Enabled: func() bool { return d.store.Any(isCreated) },
			Run:     d.delete,
		},
	}
}

func (d *driver) create(t *umpire.T) {
	name := fmt.Sprintf("prop-ep-%s-%d", d.prefix, d.nextSeq)
	taskQueue := fmt.Sprintf("prop-ep-tq-%s-%d", d.prefix, d.nextSeq)
	d.nextSeq++

	_, err := d.operator.CreateNexusEndpoint(d.ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: name,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{
					Worker: &nexuspb.EndpointTarget_Worker{
						Namespace: d.namespace,
						TaskQueue: taskQueue,
					},
				},
			},
		},
	})
	t.NoError(err)
	// World mutation happens server-side via the observer.
}

func (d *driver) describe(t *umpire.T) {
	ep := d.store.Pick(t, "endpointAny", anyEndpoint)

	resp, err := d.operator.GetNexusEndpoint(d.ctx, &operatorservice.GetNexusEndpointRequest{Id: ep.ID})
	if isDeletedErr(err, ep) {
		return
	}
	t.NoError(err)
	t.Equal(ep.Name, resp.GetEndpoint().GetSpec().GetName(),
		"endpoint %s: name mismatch", ep.Name)
}

func (d *driver) delete(t *umpire.T) {
	ep := d.store.Pick(t, "endpointCreated", isCreated)

	_, err := d.operator.DeleteNexusEndpoint(d.ctx, &operatorservice.DeleteNexusEndpointRequest{
		Id:      ep.ID,
		Version: ep.Version,
	})
	t.NoError(err)
	// Status transition + EntityDeleted recorded server-side via the observer.
}

func (d *driver) CheckInvariant(t *umpire.T) {
	for _, ep := range d.store.All() {
		_, err := d.operator.GetNexusEndpoint(d.ctx, &operatorservice.GetNexusEndpointRequest{Id: ep.ID})
		if isDeletedErr(err, ep) {
			continue
		}
		t.NoError(err)
	}
}

func (d *driver) Cleanup(t *umpire.T) {
	for _, ep := range d.store.All() {
		if ep.Status == StatusDeleted {
			continue
		}
		_, _ = d.operator.DeleteNexusEndpoint(d.ctx, &operatorservice.DeleteNexusEndpointRequest{
			Id:      ep.ID,
			Version: ep.Version,
		})
	}
}

func isCreated(ep *Entity) bool { return ep.Status == StatusCreated }

func anyEndpoint(*Entity) bool { return true }

func isDeletedErr(err error, ep *Entity) bool {
	if ep.Status != StatusDeleted {
		return false
	}
	var notFoundErr *serviceerror.NotFound
	return errors.As(err, &notFoundErr)
}
