package matching

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskFinishResult(t *testing.T) {
	t.Parallel()

	// success
	task := &internalTask{responseC: make(chan taskResponse, 1)}
	task.finish(nil)
	got, ok := task.getResponse()
	require.True(t, ok)
	require.Equal(t, taskResponse{}, got)

	// start error
	task = &internalTask{responseC: make(chan taskResponse, 1)}
	startErr := errors.New("start failed")
	task.finish(startErr)
	got, ok = task.getResponse()
	require.True(t, ok)
	require.Equal(t, taskResponse{startErr: startErr}, got)

	// drop
	task = &internalTask{responseC: make(chan taskResponse, 1)}
	task.finish(dropReasonInvalid)
	got, ok = task.getResponse()
	require.True(t, ok)
	require.Equal(t, taskResponse{dropReason: dropReasonInvalid}, got)

	// forward success
	refunded := false
	task = &internalTask{
		responseC:    make(chan taskResponse, 1),
		recycleToken: func(*internalTask) { refunded = true },
	}
	task.finishForward("response", nil)
	got, ok = task.getResponse()
	require.True(t, ok)
	require.Equal(t, taskResponse{forwarded: true, forwardRes: "response"}, got)
	require.False(t, refunded)
	require.Nil(t, task.recycleToken)

	// forward error
	task = &internalTask{responseC: make(chan taskResponse, 1)}
	forwardErr := errors.New("forward failed")
	task.finishForward(nil, forwardErr)
	got, ok = task.getResponse()
	require.True(t, ok)
	require.Equal(t, taskResponse{forwarded: true, forwardErr: forwardErr}, got)
}
