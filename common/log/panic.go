package log

import (
	"fmt"
	"runtime/debug"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log/tag"
)

// CapturePanic is used to capture panic, it will log the panic and also return the error through pointer.
// If the panic value is not error then a default error is returned
// We have to use pointer is because in golang: "recover return nil if was not called directly by a deferred function."
// And we have to set the returned error otherwise our handler will return nil as error which is incorrect
func CapturePanic(logger Logger, retError *error) {
	if panicObj := recover(); panicObj != nil {
		*retError = panicToError(logger, panicObj)
	}
}

// CapturePanicWithHook is CapturePanic plus onPanic, which is called with the captured error after a panic.
// Use it instead of a separate deferred closure that checks the error: it must still be deferred directly.
func CapturePanicWithHook(logger Logger, retError *error, onPanic func(error)) {
	//revive:disable-next-line:defer
	if panicObj := recover(); panicObj != nil {
		*retError = panicToError(logger, panicObj)
		onPanic(*retError)
	}
}

func panicToError(logger Logger, panicObj any) error {
	err, ok := panicObj.(error)
	if !ok {
		err = fmt.Errorf("panic: %v", panicObj)
	}

	st := string(debug.Stack())

	logger.Error("Panic is captured", tag.SysStackTrace(st), tag.Error(err))

	return serviceerror.NewInternal(err.Error())
}
