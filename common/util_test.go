// The MIT License (MIT)
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
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package common

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"

	workflow "github.com/uber/cadence/.gen/go/shared"
)

func TestIsServiceTransientError_ContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(100 * time.Millisecond)

	require.False(t, IsServiceTransientError(ctx.Err()))
}

func TestIsServiceTransientError_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, IsServiceTransientError(ctx.Err()))
}

func TestConvertDynamicConfigMapPropertyToIntMap(t *testing.T) {
	dcValue := make(map[string]interface{})
	for idx, value := range []interface{}{int(0), int32(1), int64(2), float64(3.0)} {
		dcValue[strconv.Itoa(idx)] = value
	}

	intMap, err := ConvertDynamicConfigMapPropertyToIntMap(dcValue)
	require.NoError(t, err)
	require.Len(t, intMap, 4)
	for i := 0; i != 4; i++ {
		require.Equal(t, i, intMap[i])
	}
}

func TestCreateHistoryStartWorkflowRequest_ExpirationTimeWithCron(t *testing.T) {
	domainId := uuid.New()
	request := &workflow.StartWorkflowExecutionRequest{
		RetryPolicy:                         &workflow.RetryPolicy{
			InitialIntervalInSeconds:    Int32Ptr(60),
			ExpirationIntervalInSeconds: Int32Ptr(60),
		},
		CronSchedule:                        StringPtr("@every 300s"),
	}
	now := time.Now()
	startRequest := CreateHistoryStartWorkflowRequest(domainId, request)

	expirationTime := startRequest.GetExpirationTimestamp()
	require.NotNil(t, expirationTime)
	require.True(t, time.Unix(0, expirationTime).Sub(now) > 60 * time.Second)
}

func TestCreateHistoryStartWorkflowRequest_ExpirationTimeWithoutCron(t *testing.T) {
	domainId := uuid.New()
	request := &workflow.StartWorkflowExecutionRequest{
		RetryPolicy:                         &workflow.RetryPolicy{
			InitialIntervalInSeconds:    Int32Ptr(60),
			ExpirationIntervalInSeconds: Int32Ptr(60),
		},
	}
	now := time.Now()
	startRequest := CreateHistoryStartWorkflowRequest(domainId, request)

	expirationTime := startRequest.GetExpirationTimestamp()
	require.NotNil(t, expirationTime)
	delta := time.Unix(0, expirationTime).Sub(now)
	require.True(t, delta > 58 * time.Second)
	require.True(t, delta < 62 * time.Second)
}

func TestConvertIndexedValueTypeToThriftType(t *testing.T) {
	expected := workflow.IndexedValueType_Values()
	for i := 0; i < len(expected); i++ {
		require.Equal(t, expected[i], ConvertIndexedValueTypeToThriftType(i, nil))
		require.Equal(t, expected[i], ConvertIndexedValueTypeToThriftType(float64(i), nil))
	}
}
