package persistence

import (
	enumspb "go.temporal.io/api/enums/v1"
)

// GetWorkspaceId returns the workspace ID from the activity's WorkspaceOptions.
// Returns empty string if no workspace is configured.
func (x *ActivityInfo) GetWorkspaceId() string {
	if x != nil && x.WorkspaceOptions != nil {
		return x.WorkspaceOptions.GetWorkspaceId()
	}
	return ""
}

// GetWorkspaceAccessMode returns the workspace access mode from the activity's WorkspaceOptions.
// Returns UNSPECIFIED if no workspace is configured.
func (x *ActivityInfo) GetWorkspaceAccessMode() enumspb.WorkspaceAccessMode {
	if x != nil && x.WorkspaceOptions != nil {
		return x.WorkspaceOptions.GetAccessMode()
	}
	return enumspb.WorkspaceAccessMode(0)
}
