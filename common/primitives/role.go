package primitives

type ServiceName string

// These constants represent service roles
const (
	AllServices             ServiceName = "all"
	FrontendService         ServiceName = "frontend"
	InternalFrontendService ServiceName = "internal-frontend"
	HistoryService          ServiceName = "history"
	MatchingService         ServiceName = "matching"
	WorkerService           ServiceName = "worker"
	ServerService           ServiceName = "server"
	UnitTestService         ServiceName = "unittest"
)
