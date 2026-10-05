package workflow

import (
	commoncallbacks "go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/retrypolicy"
)

var EnabledCallbackKinds = dynamicconfig.NewNamespaceTypedSettingWithConverter(
	"workflow.enabledCallbackKinds",
	commoncallbacks.ConvertEnabledKinds,
	[]commoncallbacks.Kind{commoncallbacks.KindNexus},
	`The list of completion callback kinds that may be attached to a workflow execution.`,
)

type Config struct {
	maxIDLengthLimit                  dynamicconfig.IntPropertyFn
	defaultWorkflowRetrySettings      dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]
	maxLinksPerRequest                dynamicconfig.IntPropertyFnWithNamespaceFilter
	linkMaxSize                       dynamicconfig.IntPropertyFnWithNamespaceFilter
	enableSignalWithStartFromWorkflow dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

func NewConfig(dc *dynamicconfig.Collection) Config {
	return Config{
		maxIDLengthLimit:                  dynamicconfig.MaxIDLengthLimit.Get(dc),
		defaultWorkflowRetrySettings:      dynamicconfig.DefaultWorkflowRetryPolicy.Get(dc),
		maxLinksPerRequest:                dynamicconfig.FrontendMaxLinksPerRequest.Get(dc),
		linkMaxSize:                       dynamicconfig.FrontendLinkMaxSize.Get(dc),
		enableSignalWithStartFromWorkflow: dynamicconfig.EnableSignalWithStartFromWorkflow.Get(dc),
	}
}
