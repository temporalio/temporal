package persistence

import (
	"errors"
	"maps"
	"slices"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

var ErrIncompatibleWorkflowMutations = errors.New("incompatible workflow mutations")

// Clone returns a detached persistence payload. In-memory tasks cannot be persisted.
func (m *WorkflowMutation) Clone() (*WorkflowMutation, error) {
	r := *m
	r.ExecutionInfo = common.CloneProto(m.ExecutionInfo)
	r.ExecutionState = common.CloneProto(m.ExecutionState)
	r.Checksum = common.CloneProto(m.Checksum)
	r.UpsertActivityInfos = common.CloneProtoMap(m.UpsertActivityInfos)
	r.UpsertTimerInfos = common.CloneProtoMap(m.UpsertTimerInfos)
	r.UpsertChildExecutionInfos = common.CloneProtoMap(m.UpsertChildExecutionInfos)
	r.UpsertRequestCancelInfos = common.CloneProtoMap(m.UpsertRequestCancelInfos)
	r.UpsertSignalInfos = common.CloneProtoMap(m.UpsertSignalInfos)
	r.UpsertChasmNodes = common.CloneProtoMap(m.UpsertChasmNodes)
	r.DeleteActivityInfos = maps.Clone(m.DeleteActivityInfos)
	r.DeleteTimerInfos = maps.Clone(m.DeleteTimerInfos)
	r.DeleteChildExecutionInfos = maps.Clone(m.DeleteChildExecutionInfos)
	r.DeleteRequestCancelInfos = maps.Clone(m.DeleteRequestCancelInfos)
	r.DeleteSignalInfos = maps.Clone(m.DeleteSignalInfos)
	r.DeleteChasmNodes = maps.Clone(m.DeleteChasmNodes)
	r.UpsertSignalRequestedIDs = maps.Clone(m.UpsertSignalRequestedIDs)
	r.DeleteSignalRequestedIDs = maps.Clone(m.DeleteSignalRequestedIDs)
	r.NewBufferedEvents = common.CloneProtoSlice(m.NewBufferedEvents)
	r.BestEffortDeleteTasks = make(map[tasks.Category][]tasks.Key, len(m.BestEffortDeleteTasks))
	for category, keys := range m.BestEffortDeleteTasks {
		r.BestEffortDeleteTasks[category] = slices.Clone(keys)
	}
	r.Tasks = make(map[tasks.Category][]tasks.Task, len(m.Tasks))
	serializer := serialization.NewSerializer()
	for category, entries := range m.Tasks {
		if category == tasks.CategoryMemoryTimer {
			return nil, ErrIncompatibleWorkflowMutations
		}
		for _, task := range entries {
			blob, err := serializer.SerializeTask(task)
			if err != nil {
				return nil, err
			}
			cloned, err := serializer.DeserializeTask(category, blob)
			if err != nil {
				return nil, err
			}
			r.Tasks[category] = append(r.Tasks[category], cloned)
		}
	}
	return &r, nil
}

// Merge composes two ordered mutations without modifying either input.
func (m *WorkflowMutation) Merge(next *WorkflowMutation) (*WorkflowMutation, error) {
	if m == nil || next == nil || m.ExecutionInfo == nil || next.ExecutionInfo == nil || m.ExecutionState == nil || next.ExecutionState == nil {
		return nil, &InvalidPersistenceRequestError{Msg: "workflow mutation is missing execution identity"}
	}
	if m.ExecutionInfo.GetNamespaceId() != next.ExecutionInfo.GetNamespaceId() ||
		m.ExecutionInfo.GetWorkflowId() != next.ExecutionInfo.GetWorkflowId() ||
		m.ExecutionState.GetRunId() != next.ExecutionState.GetRunId() ||
		m.NextEventID != next.Condition {
		return nil, ErrIncompatibleWorkflowMutations
	}
	r, err := next.Clone()
	if err != nil {
		return nil, err
	}
	first, err := m.Clone()
	if err != nil {
		return nil, err
	}
	r.Condition = m.Condition
	r.UpsertActivityInfos, r.DeleteActivityInfos = mergeActions(first.UpsertActivityInfos, first.DeleteActivityInfos, r.UpsertActivityInfos, r.DeleteActivityInfos)
	r.UpsertTimerInfos, r.DeleteTimerInfos = mergeActions(first.UpsertTimerInfos, first.DeleteTimerInfos, r.UpsertTimerInfos, r.DeleteTimerInfos)
	r.UpsertChildExecutionInfos, r.DeleteChildExecutionInfos = mergeActions(first.UpsertChildExecutionInfos, first.DeleteChildExecutionInfos, r.UpsertChildExecutionInfos, r.DeleteChildExecutionInfos)
	r.UpsertRequestCancelInfos, r.DeleteRequestCancelInfos = mergeActions(first.UpsertRequestCancelInfos, first.DeleteRequestCancelInfos, r.UpsertRequestCancelInfos, r.DeleteRequestCancelInfos)
	r.UpsertSignalInfos, r.DeleteSignalInfos = mergeActions(first.UpsertSignalInfos, first.DeleteSignalInfos, r.UpsertSignalInfos, r.DeleteSignalInfos)
	r.UpsertChasmNodes, r.DeleteChasmNodes = mergeActions(first.UpsertChasmNodes, first.DeleteChasmNodes, r.UpsertChasmNodes, r.DeleteChasmNodes)
	r.UpsertSignalRequestedIDs, r.DeleteSignalRequestedIDs = mergeActions(first.UpsertSignalRequestedIDs, first.DeleteSignalRequestedIDs, r.UpsertSignalRequestedIDs, r.DeleteSignalRequestedIDs)
	if !next.ClearBufferedEvents {
		r.NewBufferedEvents = append(first.NewBufferedEvents, r.NewBufferedEvents...)
		r.ClearBufferedEvents = m.ClearBufferedEvents
	}
	for category, entries := range first.Tasks {
		r.Tasks[category] = append(entries, r.Tasks[category]...)
	}
	for category, keys := range first.BestEffortDeleteTasks {
		r.BestEffortDeleteTasks[category] = append(keys, r.BestEffortDeleteTasks[category]...)
	}
	return r, nil
}

func mergeActions[K comparable, V any](upsert map[K]V, remove map[K]struct{}, nextUpsert map[K]V, nextRemove map[K]struct{}) (map[K]V, map[K]struct{}) {
	if upsert == nil {
		upsert = make(map[K]V)
	}
	if remove == nil {
		remove = make(map[K]struct{})
	}
	for key, value := range nextUpsert {
		upsert[key] = value
		delete(remove, key)
	}
	for key := range nextRemove {
		delete(upsert, key)
		remove[key] = struct{}{}
	}
	return upsert, remove
}
