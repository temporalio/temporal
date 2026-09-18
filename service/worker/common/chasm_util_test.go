package common

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm"
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
		execInfo := &workflowpb.WorkflowExecutionInfo{
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				sadefs.TemporalNamespaceDivision: sadefs.MustEncodeValue(
					"TemporalScheduler",
					enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				),
			}},
		}
		id, err := ArchetypeIDFromExecutionInfo(execInfo)
		require.NoError(t, err)
		require.Equal(t, chasm.WorkflowArchetypeID, id)
	})

	t.Run("CHASM", func(t *testing.T) {
		execInfo := &workflowpb.WorkflowExecutionInfo{
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				sadefs.TemporalNamespaceDivision: sadefs.MustEncodeValue(
					strconv.FormatUint(42, 10),
					enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				),
			}},
		}
		id, err := ArchetypeIDFromExecutionInfo(execInfo)
		require.NoError(t, err)
		require.Equal(t, chasm.ArchetypeID(42), id)
	})

	t.Run("ErrorOnInvalidNumber", func(t *testing.T) {
		execInfo := &workflowpb.WorkflowExecutionInfo{
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
				sadefs.TemporalNamespaceDivision: sadefs.MustEncodeValue(
					"1x",
					enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				),
			}},
		}
		_, err := ArchetypeIDFromExecutionInfo(execInfo)
		require.Error(t, err)
	})
}
