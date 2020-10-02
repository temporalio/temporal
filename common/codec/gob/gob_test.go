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

package gob

import (
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

type testStruct struct {
	Namespace  string
	WorkflowID string
	RunID      string
	StartTime  time.Time
}

func TestGobEncoder(t *testing.T) {
	encoder := NewGobEncoder()

	namespace := "test-namespace"
	wid := uuid.New()
	rid := uuid.New()
	startTime := time.Now().UTC()

	// test encode and decode 1 object
	msg := &testStruct{
		Namespace:  namespace,
		WorkflowID: wid,
		RunID:      rid,
		StartTime:  startTime,
	}
	payload, err := encoder.Encode(msg)
	require.NoError(t, err)
	var decoded *testStruct
	err = encoder.Decode(payload, &decoded)
	require.NoError(t, err)
	require.Equal(t, msg, decoded)

	// test encode and decode 2 objects
	msg2 := "test-string"
	payload, err = encoder.Encode(msg2, msg)
	require.NoError(t, err)
	var decoded2 string
	err = encoder.Decode(payload, &decoded2, &decoded)
	require.NoError(t, err)
	require.Equal(t, msg, decoded)
	require.Equal(t, msg2, decoded2)

	// test encode and decode 0 object
	_, err = encoder.Encode()
	require.Error(t, err)
	err = encoder.Decode(payload)
	require.Error(t, err)
}
