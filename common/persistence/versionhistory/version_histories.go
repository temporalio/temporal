package versionhistory

import (
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
)

// NewVersionHistories create a new instance of VersionHistories.
func NewVersionHistories(versionHistory *historyspb.VersionHistory) *historyspb.VersionHistories {
	if versionHistory == nil {
		panic("version history cannot be null")
	}

	return &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories:                  []*historyspb.VersionHistory{versionHistory},
	}
}

// Copy VersionHistories.
func CopyVersionHistories(h *historyspb.VersionHistories) *historyspb.VersionHistories {
	var histories []*historyspb.VersionHistory
	for _, history := range h.Histories {
		histories = append(histories, CopyVersionHistory(history))
	}

	return &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: h.CurrentVersionHistoryIndex,
		Histories:                  histories,
	}
}

// GetVersionHistory gets the VersionHistory according to index provided.
func GetVersionHistory(h *historyspb.VersionHistories, index int32) (*historyspb.VersionHistory, error) {
	if index < 0 || index >= int32(len(h.Histories)) {
		return nil, serviceerror.NewInternal("version histories index is out of range.")
	}

	return h.Histories[index], nil
}

// AddEmptyVersionHistory adds an empty VersionHistory to VersionHistories.
// It reuses an existing empty VersionHistory if one already exists.
// Returns:
//   - the index of the newly added or reused empty VersionHistory.
func AddEmptyVersionHistory(h *historyspb.VersionHistories) int32 {
	for idx, versionHistory := range h.Histories {
		if IsEmptyVersionHistory(versionHistory) {
			// already have an empty version history, return its index
			return int32(idx)
		}
	}
	h.Histories = append(h.Histories, &historyspb.VersionHistory{})
	return int32(len(h.Histories)) - 1
}

// AddVersionHistory adds a VersionHistory to VersionHistories.
// Returns:
//   - the index of the newly added VersionHistory
//   - error if any
func AddVersionHistory(h *historyspb.VersionHistories, v *historyspb.VersionHistory) (int32, error) {
	if v == nil {
		return 0, serviceerror.NewInternal("version histories is null.")
	}

	// assuming existing version histories inside are valid
	incomingFirstItem, err := GetFirstVersionHistoryItem(v)
	if err != nil {
		return 0, err
	}

	currentVersionHistory, err := GetVersionHistory(h, h.CurrentVersionHistoryIndex)
	if err != nil {
		return 0, err
	}
	currentFirstItem, err := GetFirstVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return 0, err
	}

	if incomingFirstItem.Version != currentFirstItem.Version {
		return 0, serviceerror.NewInternal("version history first item does not match.")
	}

	// TODO maybe we need more strict validation

	newVersionHistory := CopyVersionHistory(v)
	h.Histories = append(h.Histories, newVersionHistory)
	newVersionHistoryIndex := int32(len(h.Histories)) - 1

	return newVersionHistoryIndex, nil
}

// AddAndSwitchVersionHistory adds a VersionHistory and switch the current branch if necessary
// based on the Version of the last VersionHistoryItem.
// Returns:
//   - if the current branch has been switched or not
//   - the index of the newly added VersionHistory
//   - error if any
//
// This function should only be invoked in the event-based replication stack.
// In the state-based replication stack, a version history could be the current even if it has a smaller version
// compared to other version histories. This is because that version history could be associated with a
// state transition history with higher version.
func AddAndSwitchVersionHistory(h *historyspb.VersionHistories, v *historyspb.VersionHistory) (bool, int32, error) {
	newVersionHistoryIndex, err := AddVersionHistory(h, v)
	if err != nil {
		return false, 0, err
	}

	// check if need to switch current branch
	newLastItem, err := GetLastVersionHistoryItem(v)
	if err != nil {
		return false, 0, err
	}
	currentVersionHistory, err := GetVersionHistory(h, h.CurrentVersionHistoryIndex)
	if err != nil {
		return false, 0, err
	}
	currentLastItem, err := GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return false, 0, err
	}

	currentBranchChanged := false
	if newLastItem.Version > currentLastItem.Version {
		currentBranchChanged = true
		h.CurrentVersionHistoryIndex = newVersionHistoryIndex
	}
	return currentBranchChanged, newVersionHistoryIndex, nil
}

// FindLCAVersionHistoryItemAndIndex finds the lowest common ancestor VersionHistory index and corresponding item.
func FindLCAVersionHistoryItemAndIndex(h *historyspb.VersionHistories, incomingHistory *historyspb.VersionHistory) (*historyspb.VersionHistoryItem, int32, error) {
	var versionHistoryIndex int32
	var versionHistoryLength int32
	var versionHistoryItem *historyspb.VersionHistoryItem

	for index, localHistory := range h.Histories {
		item, err := FindLCAVersionHistoryItem(localHistory, incomingHistory)
		if err != nil {
			return nil, 0, err
		}

		// if not set
		if versionHistoryItem == nil ||
			// if seeing LCA item with higher event ID
			item.GetEventId() > versionHistoryItem.GetEventId() ||
			// if seeing LCA item with equal event ID but shorter history
			(item.GetEventId() == versionHistoryItem.GetEventId() && int32(len(localHistory.Items)) < versionHistoryLength) {

			versionHistoryIndex = int32(index)
			versionHistoryLength = int32(len(localHistory.Items))
			versionHistoryItem = item
		}
	}
	return CopyVersionHistoryItem(versionHistoryItem), versionHistoryIndex, nil
}

// FindFirstVersionHistoryIndexByVersionHistoryItem find the first VersionHistory index which contains the given version history item.
func FindFirstVersionHistoryIndexByVersionHistoryItem(h *historyspb.VersionHistories, item *historyspb.VersionHistoryItem) (int32, error) {
	for versionHistoryIndex, history := range h.Histories {
		if ContainsVersionHistoryItem(history, item) {
			return int32(versionHistoryIndex), nil
		}
	}
	return 0, serviceerror.NewInternalf("version histories does not contains given item: %v, %v", item, h)
}

// SetCurrentVersionHistoryIndex set the current VersionHistory index.
func SetCurrentVersionHistoryIndex(h *historyspb.VersionHistories, currentVersionHistoryIndex int32) error {
	if currentVersionHistoryIndex < 0 || currentVersionHistoryIndex >= int32(len(h.Histories)) {
		return serviceerror.NewInternal("invalid current version history index.")
	}

	h.CurrentVersionHistoryIndex = currentVersionHistoryIndex
	return nil
}

// GetCurrentVersionHistory gets the current VersionHistory.
func GetCurrentVersionHistory(h *historyspb.VersionHistories) (*historyspb.VersionHistory, error) {
	return GetVersionHistory(h, h.GetCurrentVersionHistoryIndex())
}

// IsCurrentVersionHistoryEmpty checks if the current VersionHistory is empty.
func IsCurrentVersionHistoryEmpty(h *historyspb.VersionHistories) (bool, error) {
	currentVersionHistory, err := GetCurrentVersionHistory(h)
	if err != nil {
		return false, err
	}
	return IsEmptyVersionHistory(currentVersionHistory), nil
}
