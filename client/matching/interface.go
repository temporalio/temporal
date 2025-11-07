package matching

import (
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/tqid"
)

type RoutingMatchingClient interface {
	matchingservice.MatchingServiceClient

	Route(p tqid.Partition) (string, error)
}
