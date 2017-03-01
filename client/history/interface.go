package history

import h "github.com/uber/cadence/.gen/go/history"

// Client is the interface exposed by history service client
type Client interface {
	h.TChanHistoryService
}
