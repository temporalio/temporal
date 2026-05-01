//go:build !experimental

package frontend

import "go.temporal.io/api/workflowservice/v1"

func startWorkflowExperimentalCheck(_ *workflowservice.StartWorkflowExecutionRequest) error {
	return nil
}
