package matching

import "go.temporal.io/server/common/tqid"

type RoutingClient interface {
	Route(p tqid.Partition) (string, error)
}
