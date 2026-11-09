//go:build !temporal_minimal || temporal_with_worker_controller

package workerdeployment

func checkWorkerControllerAvailable() error {
	return nil
}
