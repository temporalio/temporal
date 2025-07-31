package sql

import (
	"encoding/binary"
	"math"

	"github.com/dgryski/go-farm"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/primitives"
)

func getPartitionForRangeHash(rangeHash uint32, totalPartitions uint32) uint32 {
	if totalPartitions == 0 {
		return 0
	}
	return rangeHash / getPartitionBoundaryStart(1, totalPartitions)
}

func getPartitionBoundaryStart(partition uint32, totalPartitions uint32) uint32 {
	if totalPartitions == 0 {
		return 0
	}

	if partition >= totalPartitions {
		return math.MaxUint32
	}

	return uint32((float32(partition) / float32(totalPartitions)) * math.MaxUint32)
}

// nolint:revive,confusing-results // moving old code
func getBoundariesForPartition(partition uint32, totalPartitions uint32) (uint32, uint32) {
	endBoundary := getPartitionBoundaryStart(partition+1, totalPartitions)

	if endBoundary != math.MaxUint32 {
		endBoundary--
	}

	return getPartitionBoundaryStart(partition, totalPartitions), endBoundary
}

// Returns the persistence task queue id and a uint32 hash for a task queue.
func taskQueueIdAndHash(
	namespaceID primitives.UUID,
	taskQueueName string,
	taskType enumspb.TaskQueueType,
	subqueue int,
) ([]byte, uint32) {
	id := taskQueueId(namespaceID, taskQueueName, taskType, subqueue)
	return id, farm.Fingerprint32(id)
}

func taskQueueId(
	namespaceID primitives.UUID,
	taskQueueName string,
	taskType enumspb.TaskQueueType,
	subqueue int,
) []byte {
	idBytes := make([]byte, 0, 16+len(taskQueueName)+1+binary.MaxVarintLen16)
	idBytes = append(idBytes, namespaceID...)
	idBytes = append(idBytes, []byte(taskQueueName)...)

	// To ensure that different names+types+subqueue ids never collide, we mark types
	// containing subqueues with an extra high bit, and then append the subqueue id. There are
	// only a few task queue types (currently 3), so the high bits are free. (If we have more
	// fields to append, we can use the next lower bit to mark the presence of that one, etc..)
	const hasSubqueue = 0x80

	if subqueue > 0 {
		idBytes = append(idBytes, uint8(taskType)|hasSubqueue)
		idBytes = binary.AppendUvarint(idBytes, uint64(subqueue))
	} else {
		idBytes = append(idBytes, uint8(taskType))
	}

	return idBytes
}
