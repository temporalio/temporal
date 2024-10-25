// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRelocatableAttributesFetcher_Fetch(t *testing.T) {
	mutableStateAttributes := &RelocatableAttributes{
		Memo: &common.Memo{Fields: map[string]*common.Payload{
			"memoLocation": {Data: []byte("mutableState")},
		}},
		SearchAttributes: &common.SearchAttributes{IndexedFields: map[string]*common.Payload{
			"searchAttributesLocation": {Data: []byte("mutableState")},
		}},
	}
	persistenceAttributes := &RelocatableAttributes{
		Memo: &common.Memo{Fields: map[string]*common.Payload{
			"memoLocation": {Data: []byte("persistence")},
		}},
		SearchAttributes: &common.SearchAttributes{IndexedFields: map[string]*common.Payload{
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
			executionInfo := &persistence.WorkflowExecutionInfo{
				Memo:                         mutableStateAttributes.Memo.Fields,
				SearchAttributes:             mutableStateAttributes.SearchAttributes.IndexedFields,
				RelocatableAttributesRemoved: c.RelocatableAttributesRemoved,
				CloseTime:                    timestamppb.New(closeTime),
				WorkflowId:                   tests.WorkflowID,
			}
			executionState := &persistence.WorkflowExecutionState{
				RunId: tests.RunID,
			}
			namespaceEntry := tests.GlobalNamespaceEntry
			ctrl := gomock.NewController(t)
			visibilityManager := manager.NewMockVisibilityManager(ctrl)
			mutableState := NewMockMutableState(ctrl)
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
					Execution: &workflow.WorkflowExecutionInfo{
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
