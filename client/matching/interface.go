package matching

import (
	"time"

	"go.temporal.io/server/common/tqid"
)

type RoutingClient interface {
	Route(p tqid.Partition) (string, error)
	// NextRouteChange returns the earliest time after now at which the result of Route(p) may
	// change due to a gradual change that's already in progress (without a membership
	// ChangedEvent), or the zero time if there is none. The result may not actually change at
	// that time, in which case the caller should call NextRouteChange again.
	NextRouteChange(p tqid.Partition) time.Time
}
