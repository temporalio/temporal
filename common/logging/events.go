package logging

// Events
const (
	// HistoryBuilder events
	InvalidHistoryActionEventID = 1000

	// History Engine events
	HistoryEngineStarting              = 2000
	HistoryEngineStarted               = 2001
	HistoryEngineShuttingDown          = 2002
	HistoryEngineShutdown              = 2003
	PersistentStoreErrorEventID        = 2010
	HistorySerializationErrorEventID   = 2020
	DuplicateTaskEventID               = 2030
	MultipleCompletionDecisionsEventID = 2040

	// Transfer Queue Processor events
	TransferQueueProcessorStarting         = 2100
	TransferQueueProcessorStarted          = 2101
	TransferQueueProcessorShuttingDown     = 2102
	TransferQueueProcessorShutdown         = 2103
	TransferQueueProcessorShutdownTimedout = 2104

	// Shard context events
	ShardRangeUpdatedEventID = 3000

	// ShardController events
	ShardControllerStarted          = 4000
	ShardControllerShutdown         = 4001
	ShardControllerShuttingDown     = 4002
	ShardControllerShutdownTimedout = 4003
	RingMembershipChangedEvent      = 4004
	ShardClosedEvent                = 4005
	ShardItemCreated                = 4010
	ShardItemRemoved                = 4011
	ShardEngineCreating             = 4020
	ShardEngineCreated              = 4021
	ShardEngineStopping             = 4022
	ShardEngineStopped              = 4023

	// MutableSateBuilder events
	InvalidMutableStateActionEventID = 4100

	// General purpose events
	OperationFailed = 9000
)
