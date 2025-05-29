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
