package history

import (
	"github.com/temporalio/temporal/.gen/proto/historyservice"
)

// Client is the interface exposed by history service client
type Client interface {
	historyservice.HistoryServiceClient
}
