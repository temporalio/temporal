//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination partition_scaler_mock.go

package matching

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartitionScalerFactory is a pluggable interface to control partition scaling.
type PartitionScalerFactory interface {
	// New will be called for a new root partition. It should return a new PartitionScaler
	// (or nil to disable).
	New(nsName namespace.Name, tqName string, tqType enumspb.TaskQueueType) PartitionScaler
}

// PartitionScaler is an instance of a scaler for one task queue.
type PartitionScaler interface {
	// OnTasks will be called once per batch of tasks added, either sync match or async. The
	// NumTasks count may be estimated based on assumptions of load balancing across
	// partitions.
	//
	// It will be given the current partition count target and its
	// private state (optional). It returns a PartitionScalerDecision, which will usually be
	// "no change". To make a change, it should return a number in NewTarget. Zero means
	// disable managed scaling.
	//
	// Changes may be ignored if values are out of range, if changed too often, or other
	// reasons. Private state will not be updated if target doesn't also change, or if the
	// change is ignored due to rate limit or any other reason.
	//
	// It will also be called periodically with less than a full batch, or even zero, to allow
	// timely scale down when there are no/few tasks.
	OnTasks(PartitionScalerInput) PartitionScalerDecision
	// Stop will be called when unloading the partition.
	Stop()
}

type PartitionScalerInput struct {
	NumTasks      int // new tasks added since last call
	CurrentTarget int
	PrivateState  *anypb.Any
}

type PartitionScalerDecision struct {
	NoChange     bool       // if true, don't do anything
	NewTarget    int        // if zero, disable managed scaling, otherwise set partition target
	PrivateState *anypb.Any // optional private state to be stored with target
}
