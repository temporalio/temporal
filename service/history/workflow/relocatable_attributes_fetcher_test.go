package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/visibility/manager"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRelocatableAttributesFetcher_Fetch(t *testing.T) {
	mutableStateAttributes := &RelocatableAttributes{
		Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
			"memoLocation": {Data: []byte("mutableState")},
		}},
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			"searchAttributesLocation": {Data: []byte("mutableState")},
		}},
	}
	persistenceAttributes := &RelocatableAttributes{
		Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
			"memoLocation": {Data: []byte("persistence")},
		}},
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			"searchAttributesLocation": {Data: []byte("persistence")},
		}},
	}

	emptyAttributes := &RelocatableAttributes{}

	require.NotEqual(t, mutableStateAttributes.Memo, persistenceAttributes.Memo)
	require.NotEqual(t, mutableStateAttributes.SearchAttributes, persistenceAttributes.SearchAttributes)
	testErr := errors.New("test error")
	for _, c := range []*struct {
		Name                         string
		RelocatableAttributesRemoved bool
		DisableFetchFromVisibility   bool
		GetWorkflowExecutionErr      error

		ExpectedInfo *RelocatableAttributes
		ExpectedErr  error
	}{
		{
			Name:                         "CloseVisibilityTaskNotComplete",
			RelocatableAttributesRemoved: false,

			ExpectedInfo: mutableStateAttributes,
		},
		{
			Name:                         "RelocatableAttributesRemoved",
			RelocatableAttributesRemoved: true,

			ExpectedInfo: persistenceAttributes,
		},
		{
			Name:                         "RelocatableAttributesRemoved DisableFetchFromVisibility",
			RelocatableAttributesRemoved: true,
			DisableFetchFromVisibility:   true,

			ExpectedInfo: emptyAttributes,
		},
		{
			Name:                         "GetWorkflowExecutionErr",
			RelocatableAttributesRemoved: true,
			GetWorkflowExecutionErr:      testErr,

			ExpectedErr: testErr,
		},
	} {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			closeTime := time.Unix(100, 0).UTC()
			executionInfo := &persistencespb.WorkflowExecutionInfo{
				Memo:                         mutableStateAttributes.Memo.Fields,
				SearchAttributes:             mutableStateAttributes.SearchAttributes.IndexedFields,
				RelocatableAttributesRemoved: c.RelocatableAttributesRemoved,
				CloseTime:                    timestamppb.New(closeTime),
				WorkflowId:                   tests.WorkflowID,
			}
			executionState := &persistencespb.WorkflowExecutionState{
				RunId: tests.RunID,
			}
			namespaceEntry := tests.GlobalNamespaceEntry
			ctrl := gomock.NewController(t)
			visibilityManager := manager.NewMockVisibilityManager(ctrl)
			mutableState := historyi.NewMockMutableState(ctrl)
			mutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
			mutableState.EXPECT().GetNamespaceEntry().Return(namespaceEntry).AnyTimes()
			mutableState.EXPECT().GetExecutionState().Return(executionState).AnyTimes()
			if c.RelocatableAttributesRemoved && !c.DisableFetchFromVisibility {
				visibilityManager.EXPECT().GetWorkflowExecution(gomock.Any(), &manager.GetWorkflowExecutionRequest{
					NamespaceID: namespaceEntry.ID(),
					Namespace:   namespaceEntry.Name(),
					RunID:       tests.RunID,
					WorkflowID:  tests.WorkflowID,
				}).Return(&manager.GetWorkflowExecutionResponse{
					Execution: &workflowpb.WorkflowExecutionInfo{
						Memo:             persistenceAttributes.Memo,
						SearchAttributes: persistenceAttributes.SearchAttributes,
					},
				}, c.GetWorkflowExecutionErr)
			}
			ctx := context.Background()

			cfg := tests.NewDynamicConfig()
			cfg.DisableFetchRelocatableAttributesFromVisibility = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(c.DisableFetchFromVisibility)
			fetcher := RelocatableAttributesFetcherProvider(cfg, visibilityManager)
			info, err := fetcher.Fetch(ctx, mutableState)

			if c.ExpectedErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, c.ExpectedErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, c.ExpectedInfo, info)
			}
		})
	}
}
