package matching

import (
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
)

// Client is the interface exposed by matching service client
type Client interface {
	matchingservice.MatchingServiceClient
}
