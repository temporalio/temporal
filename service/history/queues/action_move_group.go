package queues

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/tasks"
)

type actionMoveGroup struct {
	maxReaderCount               int
	grouper                      Grouper
	moveGroupTaskCountBase       int
	moveGroupTaskCountMultiplier float64
	taskCategoryTag              metrics.Tag
	namespaceRegistry            namespace.Registry
	metricsHandler               metrics.Handler
	logger                       log.Logger
}

func newMoveGroupAction(
	maxReaderCount int,
	grouper Grouper,
	moveGroupTaskCountBase int,
	moveGroupTaskCountMultiplier float64,
	categoryName string,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *actionMoveGroup {
	return &actionMoveGroup{
		maxReaderCount:               maxReaderCount,
		grouper:                      grouper,
		moveGroupTaskCountBase:       moveGroupTaskCountBase,
		moveGroupTaskCountMultiplier: moveGroupTaskCountMultiplier,
		taskCategoryTag:              metrics.TaskCategoryTag(categoryName),
		namespaceRegistry:            namespaceRegistry,
		metricsHandler:               metricsHandler,
		logger:                       logger,
	}
}

func (a *actionMoveGroup) Name() string {
	return "move-group"
}

func (a *actionMoveGroup) Run(readerGroup *ReaderGroup) bool {

	// Move task groups from reader x to x+1 if the # of pending tasks for a group is higher than
	// a threshold. The threshold is calculated as:
	//   moveGroupTaskCountBase * (moveGroupTaskCountMultiplier ^ x)
	//
	// If after moving a group to reader x+1, the # of pending tasks for that group becomes higher than
	// the threshold for reader x+1, it will be moved to reader x+2 in the next iteration.

	// TODO: instead of moving task groups down by just one reader, directly move it to the reader level
	// based on the total number of pending tasks across all readers.

	moved := false
	moveGroupMinTaskCount := a.moveGroupTaskCountBase
	for readerID := DefaultReaderId; readerID+1 < int64(a.maxReaderCount); readerID++ {
		if readerID != DefaultReaderId {
			moveGroupMinTaskCount = int(float64(moveGroupMinTaskCount) * a.moveGroupTaskCountMultiplier)
		}

		reader, ok := readerGroup.ReaderByID(readerID)
		if !ok {
			continue
		}

		pendingTaskPerGroup := make(map[any]int)
		reader.WalkSlices(func(s Slice) {
			for key, pendingTaskCount := range s.TaskStats().PendingPerKey {
				pendingTaskPerGroup[key] += pendingTaskCount
			}
		})

		groupsToMove := make([]any, 0, len(pendingTaskPerGroup))
		for key, pendingTaskCount := range pendingTaskPerGroup {
			if a.metricsHandler != nil && pendingTaskCount > 0 {
				metrics.QueuePendingTasksPerNamespace.
					With(a.metricsHandler).
					Record(
						int64(pendingTaskCount),
						a.namespaceTag(key),
						a.taskCategoryTag,
					)
			}

			if pendingTaskCount >= moveGroupMinTaskCount {
				groupsToMove = append(groupsToMove, key)
				a.logger.Info("Too many pending tasks, moving group to next reader",
					tag.QueueReaderID(readerID),
					tag.Counter(pendingTaskCount),
					tag.Value(key),
				)
			}
		}

		if len(groupsToMove) == 0 {
			continue
		}

		predicateForSplit := a.grouper.Predicate(groupsToMove)

		var slicesToMove []Slice
		reader.SplitSlices(func(s Slice) ([]Slice, bool) {
			// Technically we don't need this empty scope check, but it helps avoid
			// unnecessary allocation and task movement.
			scope := s.Scope()
			splitScope, _ := scope.SplitByPredicate(predicateForSplit)
			if splitScope.IsEmpty() {
				return nil, false
			}

			split, remain := s.SplitByPredicate(predicateForSplit)
			slicesToMove = append(slicesToMove, split)
			return []Slice{remain}, true
		})

		nextReader := readerGroup.GetOrCreateReader(readerID + 1)
		nextReader.MergeSlices(slicesToMove...)
		moved = true
	}

	return moved
}

func (a *actionMoveGroup) namespaceTag(key any) metrics.Tag {
	nsID := getNamespaceIDFromGroupKey(key)
	if len(nsID) == 0 {
		return metrics.NamespaceUnknownTag()
	}

	if a.namespaceRegistry != nil {
		nsName, err := a.namespaceRegistry.GetNamespaceName(namespace.ID(nsID))
		if err == nil {
			return metrics.NamespaceTag(nsName.String())
		}
	}

	return metrics.NamespaceTag(nsID)
}

func getNamespaceIDFromGroupKey(key any) string {
	switch k := key.(type) {
	case string:
		return k
	case tasks.TaskGroupNamespaceIDAndDestination:
		return k.NamespaceID
	case interface{ GetNamespaceID() string }:
		return k.GetNamespaceID()
	default:
		return ""
	}
}
