package historydlq

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
)

type QueueKey struct {
	CategoryID    int32
	SourceCluster string
	TargetCluster string
}

// TaskIdentity refers to the original persisted task, before DLQ placement.
// Execution fields are metadata and do not participate in deduplication.
type TaskIdentity struct {
	SourceShardID int32
	TaskID        int64
	NamespaceID   string
	BusinessID    string
	RunID         string
	ArchetypeID   uint32
}

type EnqueueRequest struct {
	Queue              QueueKey
	SourceTaskIdentity TaskIdentity
	Task               *commonspb.HistoryTask
}

type MessageRef struct {
	BusinessID string
	RunID      string
	MessageID  int64
	Queue      QueueKey
	EnqueuedAt time.Time
	EntryKey   string
}

func validateQueue(queue QueueKey) error {
	if queue.CategoryID <= 0 || queue.SourceCluster == "" || queue.TargetCluster == "" {
		return serviceerror.NewInvalidArgument("history DLQ requires a queue identity")
	}
	return nil
}

func validateIdentity(queue QueueKey, source TaskIdentity) error {
	if err := validateQueue(queue); err != nil || source.SourceShardID <= 0 || source.TaskID <= 0 {
		return serviceerror.NewInvalidArgument("history DLQ requires a queue and source persisted task identity")
	}
	return nil
}

func identityHash(queue QueueKey, original TaskIdentity) ([sha256.Size]byte, error) {
	if err := validateIdentity(queue, original); err != nil {
		return [sha256.Size]byte{}, err
	}
	// Quoted strings preserve tuple boundaries, including delimiters and escapes.
	tuple := fmt.Sprintf("historydlq:v1:%d:%q:%q:%d:%d",
		queue.CategoryID, queue.SourceCluster, queue.TargetCluster, original.SourceShardID, original.TaskID)
	return sha256.Sum256([]byte(tuple)), nil
}

// SegmentBusinessID returns the stable Business ID for a queue and source shard.
func SegmentBusinessID(queue QueueKey, source TaskIdentity) (string, error) {
	if err := validateIdentity(queue, source); err != nil {
		return "", err
	}
	// Quoted strings preserve tuple boundaries, including delimiters and escapes.
	tuple := fmt.Sprintf("historydlq:segment:v0:%d:%q:%q:%d",
		queue.CategoryID, queue.SourceCluster, queue.TargetCluster, source.SourceShardID)
	hash := sha256.Sum256([]byte(tuple))
	return "historydlq:segment:v0:" + hex.EncodeToString(hash[:]), nil
}

// EntryKey returns the stable key for the original persisted task within its segment.
func EntryKey(queue QueueKey, original TaskIdentity) (string, error) {
	hash, err := identityHash(queue, original)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(hash[:]), nil
}

func messageID(queue QueueKey, original TaskIdentity, enqueuedAt time.Time) (int64, error) {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if enqueuedAt.Before(epoch) || !enqueuedAt.Before(epoch.Add(time.Duration(1<<41)*time.Millisecond)) {
		return 0, serviceerror.NewInvalidArgument("history DLQ enqueue time is outside the message ID range")
	}
	hash, err := identityHash(queue, original)
	if err != nil {
		return 0, err
	}
	elapsedMilliseconds := enqueuedAt.UnixMilli() - epoch.UnixMilli()
	hash20 := int64(binary.BigEndian.Uint32(hash[28:]) & ((1 << 20) - 1))
	return (elapsedMilliseconds << 20) | hash20, nil
}
