package log

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCapturePanic(t *testing.T) {
	fooErr := testCapture("foo")
	assert.Error(t, fooErr)
	assert.Equal(t, "panic: foo", fooErr.Error())

	barErr := testCapture(fmt.Errorf("error: %v", "bar"))
	assert.Error(t, barErr)
	assert.Equal(t, "error: bar", barErr.Error())
}

func testCapture(panicObj interface{}) (retErr error) {
	defer CapturePanic(NewNoopLogger(), &retErr)

	testPanic(panicObj)
	return nil
}

func testPanic(panicObj interface{}) {
	panic(panicObj)
}
