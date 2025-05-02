package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	commandpb "go.temporal.io/api/command/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/headers"
)

func TestDisableEagerActivityDispatchForBuggyClients(t *testing.T) {
	overrides := NewOverrides()

	type Case struct {
		sdkVersion   string
		sdkName      string
		eagerAllowed bool
	}

	cases := []Case{
		{sdkName: headers.ClientNameGoSDK, sdkVersion: "1.18.1", eagerAllowed: true},
		{sdkName: headers.ClientNameTypeScriptSDK, sdkVersion: "1.4.1", eagerAllowed: false},
		{sdkName: headers.ClientNameTypeScriptSDK, sdkVersion: "1.4.4", eagerAllowed: true},
		{sdkName: headers.ClientNamePythonSDK, sdkVersion: "0.1a1", eagerAllowed: false},
		{sdkName: headers.ClientNamePythonSDK, sdkVersion: "0.1b2", eagerAllowed: false},
		{sdkName: headers.ClientNamePythonSDK, sdkVersion: "0.1b3", eagerAllowed: true},
	}
	for _, testCase := range cases {
		ctx := headers.SetVersionsForTests(context.Background(), testCase.sdkVersion, testCase.sdkName, headers.SupportedServerVersions, headers.AllFeatures)
		req := &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				{Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{RequestEagerExecution: true}}},
			},
		}
		overrides.DisableEagerActivityDispatchForBuggyClients(ctx, req)

		assert.Equal(t, req.GetCommands()[0].GetScheduleActivityTaskCommandAttributes().GetRequestEagerExecution(), testCase.eagerAllowed)
	}
}
