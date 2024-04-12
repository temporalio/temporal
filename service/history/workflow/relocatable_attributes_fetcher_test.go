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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/tests"
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
	require.NotEqual(t, mutableStateAttributes.Memo, persistenceAttributes.Memo)
	require.NotEqual(t, mutableStateAttributes.SearchAttributes, persistenceAttributes.SearchAttributes)
	testErr := errors.New("test error")
	for _, c := range []*struct {
		Name                         string
		CloseVisibilityTaskCompleted bool
		GetWorkflowExecutionErr      error

		ExpectedInfo *RelocatableAttributes
		ExpectedErr  error
	}{
		{
			Name:                         "CloseVisibilityTaskNotComplete",
			CloseVisibilityTaskCompleted: false,

			ExpectedInfo: mutableStateAttributes,
		},
		{
			Name:                         "CloseVisibilityTaskCompleted",
			CloseVisibilityTaskCompleted: true,

			ExpectedInfo: persistenceAttributes,
		},
		{
			Name:                         "GetWorkflowExecutionErr",
			CloseVisibilityTaskCompleted: true,
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
				CloseVisibilityTaskCompleted: c.CloseVisibilityTaskCompleted,
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
			if c.CloseVisibilityTaskCompleted {
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

			fetcher := RelocatableAttributesFetcherProvider(visibilityManager)
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
