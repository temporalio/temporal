package activity

import (
	"go.temporal.io/server/common/dynamicconfig"
	commonlinks "go.temporal.io/server/common/links"
)

// linkValidator validates links attached to standalone activity executions.
type linkValidator struct {
	// Distinct per-component type: all CHASM lib fx modules provide into the same
	// container, so *commonlinks.Validator cannot be injected directly.
	*commonlinks.Validator
}

func newLinkValidator(
	maxLinksPerRequest dynamicconfig.IntPropertyFnWithNamespaceFilter,
	maxLinksPerComponent dynamicconfig.IntPropertyFnWithNamespaceFilter,
	linkMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *linkValidator {
	return &linkValidator{
		commonlinks.NewValidator(
			"an activity",
			maxLinksPerRequest,
			maxLinksPerComponent,
			linkMaxSize,
		),
	}
}
