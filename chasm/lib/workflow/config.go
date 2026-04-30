package workflow

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/retrypolicy"
)

type Config struct {
	maxIDLengthLimit             dynamicconfig.IntPropertyFn
	defaultWorkflowRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings]
	maxLinksPerRequest           dynamicconfig.IntPropertyFnWithNamespaceFilter
	linkMaxSize                  dynamicconfig.IntPropertyFnWithNamespaceFilter
	enableSystemNexusOperations  dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

func NewConfig(dc *dynamicconfig.Collection) Config {
	return Config{
		maxIDLengthLimit:             dynamicconfig.MaxIDLengthLimit.Get(dc),
		defaultWorkflowRetrySettings: dynamicconfig.DefaultWorkflowRetryPolicy.Get(dc),
		maxLinksPerRequest:           dynamicconfig.FrontendMaxLinksPerRequest.Get(dc),
		linkMaxSize:                  dynamicconfig.FrontendLinkMaxSize.Get(dc),
		enableSystemNexusOperations:  dynamicconfig.EnableSystemNexusOperations.Get(dc),
	}
}
