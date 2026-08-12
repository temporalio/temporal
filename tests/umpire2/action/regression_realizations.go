package action

const (
	RegressionResourceNamespace     = "temporal.namespace"
	RegressionResourceTaskQueue     = "temporal.task_queue"
	RegressionResourceWorker        = "temporal.worker"
	RegressionResourceNexusEndpoint = "temporal.nexus_endpoint"
	RegressionResourceFaultInjector = "temporal.fault_injector"

	RegressionPolicyNexusDrop     = "nexus.drop"
	RegressionPolicyNexusFailNext = "nexus.fail_next"

	RegressionNexusScheduleDefault                    = "nexus.schedule_default"
	RegressionNexusScheduleEmbedded                   = "nexus.schedule_embedded"
	RegressionNexusSchedule                           = "nexus.schedule"
	RegressionNexusRespondStartScheduledAsync         = "nexus.respond_start.scheduled.async"
	RegressionNexusRespondStartCompletionPendingAsync = "nexus.respond_start.completion_pending.async"
	RegressionNexusRespondStartScheduledSync          = "nexus.respond_start.scheduled.sync"
	RegressionNexusCompleteScheduled                  = "nexus.complete.scheduled"
	RegressionNexusCompleteStarted                    = "nexus.complete.started"
	RegressionNexusCompleteCallbackFailed             = "nexus.complete.callback_failed"
	RegressionNexusCancel                             = "nexus.cancel"
	RegressionNexusCancelWithRetry                    = "nexus.cancel_with_retry"
	RegressionNexusTimeout                            = "nexus.timeout"
	RegressionNexusStartNewHandler                    = "nexus.start.new_handler"
	RegressionNexusStartAttachHandler                 = "nexus.start.attach_handler"
	RegressionNexusCompleteFromHandler                = "nexus.complete_from_handler"
	RegressionNexusStartActivity                      = "nexus.start_activity"
	RegressionWorkflowComplete                        = "workflow.complete"
	RegressionWorkflowObserveRunID                    = "workflow.observe_run_id"
)

func RegressionNexusRespondStart(from, response string) string {
	return "nexus.respond_start." + from + "." + response
}

func RegressionNexusComplete(from string) string {
	return "nexus.complete." + from
}
