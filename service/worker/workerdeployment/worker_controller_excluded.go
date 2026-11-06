//go:build temporal_minimal && !temporal_with_worker_controller

package workerdeployment

import "go.temporal.io/api/serviceerror"

func checkWorkerControllerAvailable() error {
	return serviceerror.NewFailedPrecondition("worker controller is not compiled into this server; rebuild with the temporal_with_worker_controller build tag")
}
