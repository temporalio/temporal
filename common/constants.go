package common

const (
	// FirstEventID is the id of the first event in the history
	FirstEventID int64 = 1
	// EmptyEventID is the id of the empty event
	EmptyEventID int64 = -23
)

const (
	// FrontendServiceName is the name of the frontend service
	FrontendServiceName = "cadence-frontend"
	// HistoryServiceName is the name of the history service
	HistoryServiceName = "cadence-history"
	// MatchingServiceName is the name of the matching service
	MatchingServiceName = "cadence-matching"
)

// Data encoding types
const (
	EncodingTypeJSON EncodingType = "json"
	EncodingTypeGob               = "gob"
)

type (
	// EncodingType is an enum that represents various data encoding types
	EncodingType string
)