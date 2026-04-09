package resource

import "go.temporal.io/server/common/primitives"

func IsSingleProcessServiceSet(serviceNames ServiceNames) bool {
	if len(serviceNames) == 0 {
		return false
	}

	_, hasFrontend := serviceNames[primitives.FrontendService]
	_, hasMatching := serviceNames[primitives.MatchingService]
	_, hasHistory := serviceNames[primitives.HistoryService]
	_, hasWorker := serviceNames[primitives.WorkerService]

	return hasFrontend && hasMatching && hasHistory && hasWorker
}
