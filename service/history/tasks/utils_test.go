package tasks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTimerTaskEventID_TimeSkippingTimerTask(t *testing.T) {
	t.Parallel()

	const eventID = int64(123)
	task := &TimeSkippingTimerTask{EventID: eventID}

	gotEventID, ok := GetTimerTaskEventID(task)
	require.True(t, ok)
	require.Equal(t, eventID, gotEventID)
}
