package nexusoperation

import (
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/links"
)

// linkValidator validates links attached to standalone Nexus operation executions.
type linkValidator struct {
	// Distinct per-component type: all CHASM lib fx modules provide into the same
	// container, so *commonlinks.Validator cannot be injected directly.
	*links.Validator
}

func newLinkValidator(
	maxLinksPerRequest dynamicconfig.IntPropertyFnWithNamespaceFilter,
	maxLinksPerComponent dynamicconfig.IntPropertyFnWithNamespaceFilter,
	linkMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *linkValidator {
	return &linkValidator{
		links.NewValidator(
			"a nexus operation",
			maxLinksPerRequest,
			maxLinksPerComponent,
			linkMaxSize,
		),
	}
}

// linkValidatorProvider builds the linkValidator from dynamic config.
func linkValidatorProvider(dc *dynamicconfig.Collection) *linkValidator {
	return newLinkValidator(
		dynamicconfig.FrontendMaxLinksPerRequest.Get(dc),
		dynamicconfig.MaxLinksPerComponent.Get(dc),
		dynamicconfig.FrontendLinkMaxSize.Get(dc),
	)
}
