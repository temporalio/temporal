// Package serviceclient defines the types under which the history and matching service clients are
// provided for dependency injection. It has no dependencies on the rest of common/resource, so that
// code that only needs a client does not depend on everything that provides one.
package serviceclient

import (
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

type (
	HistoryRawClient historyservice.HistoryServiceClient
	HistoryClient    historyservice.HistoryServiceClient

	MatchingRawClient matchingservice.MatchingServiceClient
	MatchingClient    matchingservice.MatchingServiceClient
)
