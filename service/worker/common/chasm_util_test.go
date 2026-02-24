package common

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func TestArchetypeIDFromExecutionInfo(t *testing.T) {
	t.Run("NoSearchAttributes", func(t *testing.T) {
		execInfo := &workflowpb.WorkflowExecutionInfo{}
		id, err := ArchetypeIDFromExecutionInfo(execInfo)
		require.NoError(t, err)
		require.Equal(t, chasm.WorkflowArchetypeID, id)
	})

	t.Run("NoNamespaceDivision", func(t *testing.T) {
		execInfo := &workflowpb.WorkflowExecutionInfo{
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{}},
		}
		id, err := ArchetypeIDFromExecutionInfo(execInfo)
		require.NoError(t, err)
		require.Equal(t, chasm.WorkflowArchetypeID, id)
	})

	t.Run("Scheduler", func(t *testing.T) {
		p := payload.EncodeString("TemporalScheduler")
		execInfo := &workflowpb.WorkflowExecutionInfo{
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				sadefs.TemporalNamespaceDivision: p,
			}},
		}
		id, err := ArchetypeIDFromExecutionInfo(execInfo)
		require.NoError(t, err)
		require.Equal(t, chasm.WorkflowArchetypeID, id)
	})

	t.Run("CHASM", func(t *testing.T) {
		p := payload.EncodeString(strconv.FormatUint(42, 10))
		execInfo := &workflowpb.WorkflowExecutionInfo{
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				sadefs.TemporalNamespaceDivision: p,
			}},
		}
		id, err := ArchetypeIDFromExecutionInfo(execInfo)
		require.NoError(t, err)
		require.Equal(t, chasm.ArchetypeID(42), id)
	})

	t.Run("ErrorOnInvalidNumber", func(t *testing.T) {
		p := payload.EncodeString("1x")

		execInfo := &workflowpb.WorkflowExecutionInfo{
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				sadefs.TemporalNamespaceDivision: p,
			}},
		}
		_, err := ArchetypeIDFromExecutionInfo(execInfo)
		require.Error(t, err)
	})
}
