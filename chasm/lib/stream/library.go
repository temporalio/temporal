package stream

import (
	"go.temporal.io/server/chasm"
)

// LibraryName is the registered chasm library name for native streams.
const LibraryName = "stream"

// Component names registered with the chasm framework.
const (
	StreamComponentName       = "Stream"
	SubscriptionComponentName = "Subscription"
)

// Task names registered with the chasm framework.
const (
	SweepExpiredTaskName        = "sweep_expired"
	AbortCleanupTaskName        = "abort_cleanup"
	CloseCleanupTaskName        = "close_cleanup"
	OwnerWorkflowCloseTaskName  = "owner_workflow_close"
	PublisherDedupSweepTaskName = "publisher_dedup_sweep"
	DeliveryTaskName            = "delivery"
)

// Library is the chasm library registration for native streams.  Wires
// together the handler, the component types, and the task handlers per
// the same pattern as chasm/lib/scheduler.
type Library struct {
	chasm.UnimplementedLibrary

	SweepExpiredTaskHandler        *SweepExpiredTaskHandler
	AbortCleanupTaskHandler        *AbortCleanupTaskHandler
	CloseCleanupTaskHandler        *CloseCleanupTaskHandler
	OwnerWorkflowCloseTaskHandler  *OwnerWorkflowCloseTaskHandler
	PublisherDedupSweepTaskHandler *PublisherDedupSweepTaskHandler
	DeliveryTaskHandler            *DeliveryTaskHandler
}

// NewLibrary constructs a Library with all task handlers wired in.  Used
// by the fx graph; see fx.go.
func NewLibrary(
	sweepExpired *SweepExpiredTaskHandler,
	abortCleanup *AbortCleanupTaskHandler,
	closeCleanup *CloseCleanupTaskHandler,
	ownerWorkflowClose *OwnerWorkflowCloseTaskHandler,
	publisherDedupSweep *PublisherDedupSweepTaskHandler,
	delivery *DeliveryTaskHandler,
) *Library {
	return &Library{
		SweepExpiredTaskHandler:        sweepExpired,
		AbortCleanupTaskHandler:        abortCleanup,
		CloseCleanupTaskHandler:        closeCleanup,
		OwnerWorkflowCloseTaskHandler:  ownerWorkflowClose,
		PublisherDedupSweepTaskHandler: publisherDedupSweep,
		DeliveryTaskHandler:            delivery,
	}
}

// NewNilLibrary returns a Library with no task handlers.  Used in
// registration-only contexts (tdbg, tests that don't execute tasks).
func NewNilLibrary() *Library {
	return &Library{}
}

func (l *Library) Name() string {
	return LibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Stream](
			StreamComponentName,
			chasm.WithBusinessIDAlias("StreamId"),
		),
		chasm.NewRegistrableComponent[*Subscription](
			SubscriptionComponentName,
		),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(SweepExpiredTaskName, l.SweepExpiredTaskHandler),
		chasm.NewRegistrableSideEffectTask(AbortCleanupTaskName, l.AbortCleanupTaskHandler),
		chasm.NewRegistrablePureTask(CloseCleanupTaskName, l.CloseCleanupTaskHandler),
		chasm.NewRegistrableSideEffectTask(OwnerWorkflowCloseTaskName, l.OwnerWorkflowCloseTaskHandler),
		chasm.NewRegistrablePureTask(PublisherDedupSweepTaskName, l.PublisherDedupSweepTaskHandler),
		chasm.NewRegistrableSideEffectTask(DeliveryTaskName, l.DeliveryTaskHandler),
	}
}
