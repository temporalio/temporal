//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination lane_feedback_mock.go

package replication

import (
	"google.golang.org/protobuf/types/known/anypb"
)

type (
	// LaneFeedbackProvider supplies what a lane stream receiver reports to its sender for the
	// sender's lane policy. One provider serves every stream receiver on the host, so it must
	// be safe for concurrent use, and it must answer from memory, without blocking or I/O.
	LaneFeedbackProvider interface {
		// RecordTask records a HIGH priority task received for a namespace on the local shard.
		RecordTask(shardID int32, namespaceID string)
		// ThrottledNamespaceIDs returns the namespaces overwhelming the HIGH priority traffic
		// of the local shard. They are sent as throttle_high_namespace_ids.
		ThrottledNamespaceIDs(shardID int32) []string
		// Feedback returns a snapshot for the sender's lane policy on the local shard, or nil.
		// maxBytes bounds the serialized Any, including its type URL. The provider should
		// prioritize relevant signals within that budget and return nil if no meaningful
		// payload fits. It must not truncate serialized bytes or mutate the returned snapshot.
		// The receiver drops oversized feedback and still sends the rest of the ack.
		Feedback(shardID int32, maxBytes int) *anypb.Any
	}

	noopLaneFeedbackProvider struct{}
)

// NewNoopLaneFeedbackProvider returns the open-source LaneFeedbackProvider, which reports
// nothing.
func NewNoopLaneFeedbackProvider() LaneFeedbackProvider {
	return noopLaneFeedbackProvider{}
}

func (noopLaneFeedbackProvider) RecordTask(int32, string) {}

func (noopLaneFeedbackProvider) ThrottledNamespaceIDs(int32) []string { return nil }

func (noopLaneFeedbackProvider) Feedback(int32, int) *anypb.Any { return nil }
