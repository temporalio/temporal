package log

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapturePanic(t *testing.T) {
	fooErr := testCapture("foo")
	assert.Error(t, fooErr)
	assert.Equal(t, "panic: foo", fooErr.Error())

	barErr := testCapture(fmt.Errorf("error: %v", "bar"))
	assert.Error(t, barErr)
	assert.Equal(t, "error: bar", barErr.Error())
}

func testCapture(panicObj any) (retErr error) {
	defer CapturePanic(NewNoopLogger(), &retErr)

	testPanic(panicObj)
	return nil
}

func testPanic(panicObj any) {
	panic(panicObj)
}

func TestCapturePanicWithHook(t *testing.T) {
	var hookErr error
	retErr := func() (retErr error) {
		defer CapturePanicWithHook(NewNoopLogger(), &retErr, func(err error) { hookErr = err })
		testPanic("foo")
		return nil
	}()
	require.EqualError(t, retErr, "panic: foo")
	require.Equal(t, retErr, hookErr)

	hookCalled := false
	retErr = func() (retErr error) {
		defer CapturePanicWithHook(NewNoopLogger(), &retErr, func(error) { hookCalled = true })
		return nil
	}()
	require.NoError(t, retErr)
	require.False(t, hookCalled)
}
