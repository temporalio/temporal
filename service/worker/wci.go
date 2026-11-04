//go:build !temporal_minimal || temporal_with_worker_controller

package worker

import wcicomponent "go.temporal.io/auto-scaled-workers/wci/workercomponent"

var wciComponentModule = wcicomponent.Module
