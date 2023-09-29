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

package client_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
)

type (
	testDataStoreFactory struct {
		client.DataStoreFactory
		err             error
		enqueueRequests int
		readRequests    int
	}
	testQueueV2 struct {
		enqueueRequests *int
		readRequests    *int
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

func (t *testDataStoreFactory) NewQueueV2() (persistence.QueueV2, error) {
	if t.err != nil {
		return nil, t.err
	}
	return &testQueueV2{
		enqueueRequests: &t.enqueueRequests,
		readRequests:    &t.readRequests,
	}, nil
}

func TestFaultInjectionDataStoreFactory_NewQueueV2_CreateErr(t *testing.T) {
	// Tests that we propagate errors from the base data store factory when creating a QueueV2

	t.Parallel()

	errCreate := errors.New("error creating QueueV2")
	dataStoreFactory := &testDataStoreFactory{
		err: errCreate,
	}
	factory := client.NewFaultInjectionDatastoreFactory(&config.FaultInjection{
		Rate:    1.0,
		Targets: config.FaultInjectionTargets{},
	}, dataStoreFactory)

	_, err := factory.NewQueueV2()
	assert.ErrorIs(t, err, errCreate)
}

func TestFaultInjectionDataStoreFactory_NewQueueV2_FlatErrorRate(t *testing.T) {
	// Tests fault injection for QueueV2 with a flat error rate across all methods.

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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			faultInjectionConfig := &config.FaultInjection{
				Rate:    tc.errorRate,
				Targets: config.FaultInjectionTargets{},
			}
			expectErr := tc.expectErr

			testFaultInjectionConfig(t, faultInjectionConfig, expectErr)
		})
	}
}

func TestFaultInjectionDataStoreFactory_NewQueueV2_MethodConfig(t *testing.T) {
	// Tests fault injection for QueueV2 with a per-method error config and no flat error rate.

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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			methodConfig := config.FaultInjectionMethodConfig{
				Errors: map[string]float64{
					"ResourceExhausted": tc.errorRate,
				},
			}
			faultInjectionConfig := &config.FaultInjection{
				Rate: 0.0,
				Targets: config.FaultInjectionTargets{
					DataStores: map[config.DataStoreName]config.FaultInjectionDataStoreConfig{
						config.QueueV2Name: {
							Methods: map[string]config.FaultInjectionMethodConfig{
								"EnqueueMessage": methodConfig,
								"ReadMessages":   methodConfig,
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
	factory := client.NewFaultInjectionDatastoreFactory(faultInjectionConfig, dataStoreFactory)

	q, err := factory.NewQueueV2()
	require.NoError(t, err)

	_, err = q.EnqueueMessage(context.Background(), nil)
	if expectErr {
		assert.Error(t, err)
		assert.Zero(t, dataStoreFactory.enqueueRequests)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, 1, dataStoreFactory.enqueueRequests)
	}

	_, err = q.ReadMessages(context.Background(), nil)
	if expectErr {
		assert.Error(t, err)
		assert.Zero(t, dataStoreFactory.readRequests)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, 1, dataStoreFactory.readRequests)
	}

	q2, err := factory.NewQueueV2()
	require.NoError(t, err)
	assert.Equal(t, q, q2, "NewQueueV2 should cache previous result")
}
