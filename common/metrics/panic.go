package metrics

import (
	"runtime/debug"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// CapturePanic is used to capture panic, it will emit the service panic metric, log the panic and also return the error through pointer.
// If the panic value is not error then a default error is returned
// We have to use pointer is because in golang: "recover return nil if was not called directly by a deferred function."
// And we have to set the returned error otherwise our handler will return nil as error which is incorrect
func CapturePanic(logger log.Logger, metricHandler Handler, retError *error) {
	//revive:disable-next-line:defer
	if pObj := recover(); pObj != nil {
		err, ok := pObj.(error)
		if !ok {
			err = serviceerror.NewInternalf("panic: %v", pObj)
		}

		st := string(debug.Stack())

		logger.Error("Panic is captured", tag.SysStackTrace(st), tag.Error(err))

		ServicePanic.With(metricHandler).Record(1)
		*retError = serviceerror.NewInternal(err.Error())
	}
}
