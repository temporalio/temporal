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

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mock"
)

type (
	testDataStoreFactory struct {
		persistence.DataStoreFactory
		err                 error
		enqueueRequests     int
		readRequests        int
		createRequests      int
		rangeDeleteRequests int
	}
	testQueueV2 struct {
		enqueueRequests     *int
		readRequests        *int
		createRequests      *int
		rangeDeleteRequests *int
		listQueuesRequests  *int
	}
)

func (t *testQueueV2) EnqueueMessage(
	context.Context,
	*persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	*t.enqueueRequests++
	return nil, nil
}

func (t *testQueueV2) ReadMessages(
	context.Context,
	*persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	*t.readRequests++
	return nil, nil
}

func (t *testQueueV2) CreateQueue(
	context.Context,
	*persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	*t.createRequests++
	return nil, nil
}

func (t *testQueueV2) RangeDeleteMessages(
	context.Context,
	*persistence.InternalRangeDeleteMessagesRequest,
) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	*t.rangeDeleteRequests++
	return nil, nil
}

func (t *testQueueV2) ListQueues(
	context.Context,
	*persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	*t.listQueuesRequests++
	return nil, nil
}

func (t *testDataStoreFactory) NewQueueV2() (persistence.QueueV2, error) {
	if t.err != nil {
		return nil, t.err
	}
	return &testQueueV2{
		enqueueRequests:     &t.enqueueRequests,
		readRequests:        &t.readRequests,
		createRequests:      &t.createRequests,
		rangeDeleteRequests: &t.rangeDeleteRequests,
	}, nil
}

func TestFaultInjectionDataStoreFactory_NewQueueV2_CreateErr(t *testing.T) {
	// Tests that we propagate errors from the base data store factory when creating a QueueV2

	t.Parallel()

	ctrl := gomock.NewController(t)
	dataStoreFactory := mock.NewMockDataStoreFactory(ctrl)

	errCreate := errors.New("error creating QueueV2")
	dataStoreFactory.EXPECT().NewQueueV2().Return(nil, errCreate).Times(1)

	factory := NewFaultInjectionDatastoreFactory(&config.FaultInjection{}, dataStoreFactory)

	_, err := factory.NewQueueV2()
	assert.ErrorIs(t, err, errCreate)
}

func TestFaultInjectionDataStoreFactory_NewQueueV2_MethodConfig(t *testing.T) {
	// Tests fault injection for QueueV2.

	t.Parallel()

	for _, tc := range []struct {
		name      string
		errorRate float64
		expectErr bool
	}{
		{
			name:      "No errors",
			errorRate: 0.0,
			expectErr: false,
		},
		{
			name:      "All errors",
			errorRate: 1.0,
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			methodConfig := config.FaultInjectionMethodConfig{
				Errors: map[string]float64{
					"ResourceExhausted": tc.errorRate,
				},
			}
			faultInjectionConfig := &config.FaultInjection{
				Targets: config.FaultInjectionTargets{
					DataStores: map[config.DataStoreName]config.FaultInjectionDataStoreConfig{
						config.QueueV2Name: {
							Methods: map[string]config.FaultInjectionMethodConfig{
								"EnqueueMessage":      methodConfig,
								"ReadMessages":        methodConfig,
								"CreateQueue":         methodConfig,
								"RangeDeleteMessages": methodConfig,
							},
						},
					},
				},
			}
			expectErr := tc.expectErr

			testFaultInjectionConfig(t, faultInjectionConfig, expectErr)
		})
	}
}

func testFaultInjectionConfig(t *testing.T, faultInjectionConfig *config.FaultInjection, expectErr bool) {
	t.Helper()

	dataStoreFactory := &testDataStoreFactory{}
	factory := NewFaultInjectionDatastoreFactory(faultInjectionConfig, dataStoreFactory)

	q, err := factory.NewQueueV2()
	require.NoError(t, err)

	_, err = q.EnqueueMessage(context.Background(), nil)
	verifyMethod(t, expectErr, err, dataStoreFactory.enqueueRequests)

	_, err = q.ReadMessages(context.Background(), nil)
	verifyMethod(t, expectErr, err, dataStoreFactory.readRequests)

	_, err = q.CreateQueue(context.Background(), nil)
	verifyMethod(t, expectErr, err, dataStoreFactory.createRequests)

	_, err = q.RangeDeleteMessages(context.Background(), nil)
	verifyMethod(t, expectErr, err, dataStoreFactory.rangeDeleteRequests)

	q2, err := factory.NewQueueV2()
	require.NoError(t, err)
	assert.Equal(t, q, q2, "NewQueueV2 should cache previous result")
}

func verifyMethod(t *testing.T, expectErr bool, err error, requests int) {
	t.Helper()
	if expectErr {
		assert.Error(t, err)
		assert.Zero(t, requests)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, 1, requests)
	}
}
