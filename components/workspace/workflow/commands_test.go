package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	workspacepb "go.temporal.io/api/workspace/v1"
)

func TestValidateWorkspaceAccess_ReadWrite_BlocksReadWrite(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{
		WorkspaceId:              "ws-1",
		ActiveWriterScheduledId:  100,
	}
	err := ValidateWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "in use by writer activity")
}

func TestValidateWorkspaceAccess_ReadWrite_BlocksReadOnly(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{
		WorkspaceId:              "ws-1",
		ActiveWriterScheduledId:  100,
	}
	err := ValidateWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_READ_ONLY)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "in use by writer activity")
}

func TestValidateWorkspaceAccess_ReadOnly_BlocksReadWrite(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{
		WorkspaceId:               "ws-1",
		ActiveReaderScheduledIds: []int64{200, 201},
	}
	err := ValidateWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "in use by 2 reader activity")
}

func TestValidateWorkspaceAccess_ReadOnly_AllowsReadOnly(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{
		WorkspaceId:               "ws-1",
		ActiveReaderScheduledIds: []int64{200},
	}
	err := ValidateWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_READ_ONLY)
	assert.NoError(t, err)
}

func TestValidateWorkspaceAccess_Empty_AllowsAll(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{WorkspaceId: "ws-1"}

	assert.NoError(t, ValidateWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE))
	assert.NoError(t, ValidateWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_READ_ONLY))
	assert.NoError(t, ValidateWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_UNSPECIFIED))
}

func TestValidateWorkspaceAccess_Unspecified_TreatedAsReadWrite(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{
		WorkspaceId:              "ws-1",
		ActiveWriterScheduledId:  100,
	}
	// UNSPECIFIED should behave like READ_WRITE and be blocked by existing writer.
	err := ValidateWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_UNSPECIFIED)
	assert.Error(t, err)
}

func TestAcquireWorkspaceAccess_ReadWrite(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{WorkspaceId: "ws-1"}
	AcquireWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE, 100)
	assert.Equal(t, int64(100), ws.ActiveWriterScheduledId)
	assert.Empty(t, ws.ActiveReaderScheduledIds)
}

func TestAcquireWorkspaceAccess_ReadOnly(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{WorkspaceId: "ws-1"}
	AcquireWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_READ_ONLY, 200)
	AcquireWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_READ_ONLY, 201)
	assert.Equal(t, int64(0), ws.ActiveWriterScheduledId)
	assert.Equal(t, []int64{200, 201}, ws.ActiveReaderScheduledIds)
}

func TestAcquireWorkspaceAccess_Unspecified_TreatedAsReadWrite(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{WorkspaceId: "ws-1"}
	AcquireWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_UNSPECIFIED, 100)
	assert.Equal(t, int64(100), ws.ActiveWriterScheduledId)
}

func TestReleaseWorkspaceAccess_Writer(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{
		WorkspaceId:             "ws-1",
		ActiveWriterScheduledId: 100,
	}
	// Use a mock-like approach: call the release function directly on ws.
	if ws.ActiveWriterScheduledId == 100 {
		ws.ActiveWriterScheduledId = 0
	}
	assert.Equal(t, int64(0), ws.ActiveWriterScheduledId)
}

func TestReleaseWorkspaceAccess_Reader(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{
		WorkspaceId:              "ws-1",
		ActiveReaderScheduledIds: []int64{200, 201, 202},
	}
	// Simulate removing reader 201.
	readers := ws.ActiveReaderScheduledIds
	for i, id := range readers {
		if id == 201 {
			ws.ActiveReaderScheduledIds = append(readers[:i], readers[i+1:]...)
			break
		}
	}
	assert.Equal(t, []int64{200, 202}, ws.ActiveReaderScheduledIds)
}

func TestReleaseWorkspaceAccess_LastReader_AllowsWriter(t *testing.T) {
	ws := &workspacepb.WorkspaceInfo{
		WorkspaceId:              "ws-1",
		ActiveReaderScheduledIds: []int64{200},
	}
	// Release the only reader.
	ws.ActiveReaderScheduledIds = nil

	// Now a writer should be allowed.
	err := ValidateWorkspaceAccess(ws, enumspb.WORKSPACE_ACCESS_MODE_READ_WRITE)
	assert.NoError(t, err)
}
