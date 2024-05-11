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

package faultinjection

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
)

func TestFaultInjection_DataStoreFactory_CreateErr(t *testing.T) {
	// Tests that we propagate errors from the base data store factory when creating a QueueV2

	t.Parallel()

	ctrl := gomock.NewController(t)
	dataStoreFactory := mock.NewMockDataStoreFactory(ctrl)

	errCreate := errors.New("error creating QueueV2")
	dataStoreFactory.EXPECT().NewQueueV2().Return(nil, errCreate)

	factory := NewFaultInjectionDatastoreFactory(&config.FaultInjection{}, dataStoreFactory)

	_, err := factory.NewQueueV2()
	assert.ErrorIs(t, err, errCreate)
}

func TestFaultInjection_Inject(t *testing.T) {
	t.Parallel()

	verifyError := func(t *testing.T, expectErr bool, err error) {
		t.Helper()
		if expectErr {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "fault injection error")
			var reErr *serviceerror.ResourceExhausted
			if errors.As(err, &reErr) {
				assert.ErrorAs(t, err, &reErr)
			} else {
				var timeoutErr *persistence.TimeoutError
				assert.ErrorAs(t, err, &timeoutErr)
			}
		} else {
			assert.NoError(t, err)
		}
	}

	for _, tc := range []struct {
		name         string
		methodConfig config.FaultInjectionMethodConfig
		expectErr    bool
	}{
		{
			name:         "No errors",
			methodConfig: config.FaultInjectionMethodConfig{},
			expectErr:    false,
		},
		{
			name: "All errors",
			methodConfig: config.FaultInjectionMethodConfig{
				Errors: map[string]float64{
					"ResourceExhausted": 0.3,
					"Timeout":           0.7,
				}},
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			faultInjectionConfig := &config.FaultInjection{
				Targets: config.FaultInjectionTargets{
					DataStores: map[config.DataStoreName]config.FaultInjectionDataStoreConfig{
						config.QueueV2Name: {
							Methods: map[string]config.FaultInjectionMethodConfig{
								"EnqueueMessage":      tc.methodConfig,
								"ReadMessages":        tc.methodConfig,
								"CreateQueue":         tc.methodConfig,
								"RangeDeleteMessages": tc.methodConfig,
							},
						},
					},
				},
			}

			ctrl := gomock.NewController(t)
			baseFactory := mock.NewMockDataStoreFactory(ctrl)
			factory := NewFaultInjectionDatastoreFactory(faultInjectionConfig, baseFactory)
			baseQueue := mock.NewMockQueueV2(ctrl)
			baseFactory.EXPECT().NewQueueV2().Return(baseQueue, nil)

			q, err := factory.NewQueueV2()
			require.NoError(t, err)

			baseQueueTimes := 0
			if !tc.expectErr {
				baseQueueTimes = 1
			}

			baseQueue.EXPECT().EnqueueMessage(gomock.Any(), gomock.Any()).Return(nil, nil).Times(baseQueueTimes)
			_, err = q.EnqueueMessage(context.Background(), nil)
			verifyError(t, tc.expectErr, err)

			baseQueue.EXPECT().ReadMessages(gomock.Any(), gomock.Any()).Return(nil, nil).Times(baseQueueTimes)
			_, err = q.ReadMessages(context.Background(), nil)
			verifyError(t, tc.expectErr, err)

			baseQueue.EXPECT().CreateQueue(gomock.Any(), gomock.Any()).Return(nil, nil).Times(baseQueueTimes)
			_, err = q.CreateQueue(context.Background(), nil)
			verifyError(t, tc.expectErr, err)

			baseQueue.EXPECT().RangeDeleteMessages(gomock.Any(), gomock.Any()).Return(nil, nil).Times(baseQueueTimes)
			_, err = q.RangeDeleteMessages(context.Background(), nil)
			verifyError(t, tc.expectErr, err)

			// List queues is not configured for FI.
			baseQueue.EXPECT().ListQueues(gomock.Any(), gomock.Any()).Return(nil, nil)
			_, err = q.ListQueues(context.Background(), nil)
			verifyError(t, false, err)

			q2, err := factory.NewQueueV2()
			require.NoError(t, err)
			assert.Equal(t, q, q2, "NewQueueV2 should cache previous result")
		})
	}
}

func TestFaultInjection_StoreNotConfigured(t *testing.T) {
	t.Parallel()

	faultInjectionConfig := &config.FaultInjection{
		Targets: config.FaultInjectionTargets{
			DataStores: map[config.DataStoreName]config.FaultInjectionDataStoreConfig{
				config.QueueV2Name: {},
			},
		},
	}

	ctrl := gomock.NewController(t)
	baseFactory := mock.NewMockDataStoreFactory(ctrl)
	factory := NewFaultInjectionDatastoreFactory(faultInjectionConfig, baseFactory)
	baseQueue := mock.NewMockQueueV2(ctrl)
	baseFactory.EXPECT().NewQueueV2().Return(baseQueue, nil)

	baseExecutionStore := mock.NewMockExecutionStore(ctrl)
	baseFactory.EXPECT().NewExecutionStore().Return(baseExecutionStore, nil)

	q, err := factory.NewQueueV2()
	require.NoError(t, err, "store with empty configuration shouldn't use fault injection")

	baseQueue.EXPECT().EnqueueMessage(gomock.Any(), gomock.Any()).Return(&persistence.InternalEnqueueMessageResponse{}, nil)
	resp1, err := q.EnqueueMessage(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp1)

	e, err := factory.NewExecutionStore()
	require.NoError(t, err, "store with missing configuration shouldn't use fault injection")
	baseExecutionStore.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.InternalCreateWorkflowExecutionResponse{}, nil)
	resp2, err := e.CreateWorkflowExecution(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, resp2)
}
