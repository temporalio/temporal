package frontend

import (
	"context"

	"github.com/blang/semver/v4"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/headers"
)

// Overrides defines a set of special case behaviors like compensating for buggy
// SDK implementations
type Overrides struct {
	minTypeScriptEagerActivitySupportedVersion semver.Version
}

func NewOverrides() *Overrides {
	return &Overrides{
		minTypeScriptEagerActivitySupportedVersion: semver.MustParse("1.4.4"),
	}
}

func (o *Overrides) shouldForceDisableEagerDispatch(sdkName, sdkVersion string) bool {
	if sdkName == headers.ClientNamePythonSDK && (sdkVersion == "0.1a1" || sdkVersion == "0.1a2" || sdkVersion == "0.1b1" || sdkVersion == "0.1b2") {
		return true
	} else if sdkName == headers.ClientNameTypeScriptSDK {
		ver, err := semver.Parse(sdkVersion)
		// Don't bother with non semver
		if err != nil {
			return false
		}
		return ver.LT(o.minTypeScriptEagerActivitySupportedVersion)
	}
	return false
}

func (o *Overrides) disableEagerDispatch(
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) {
	for _, cmd := range request.GetCommands() {
		attrs := cmd.GetScheduleActivityTaskCommandAttributes()
		if attrs != nil {
			attrs.RequestEagerExecution = false
		}
	}
}

// DisableEagerActivityDispatchForBuggyClients compensates for SDK versions
// that have buggy implementations of eager activity dispatch
func (o *Overrides) DisableEagerActivityDispatchForBuggyClients(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) {
	sdkName, sdkVersion := headers.GetClientNameAndVersion(ctx)
	if o.shouldForceDisableEagerDispatch(sdkName, sdkVersion) {
		o.disableEagerDispatch(request)
	}
}
