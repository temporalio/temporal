package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
)

// linkValidator validates links attached to standalone Nexus operation executions.
type linkValidator struct {
	// Wrapped so that nexusoperation.linkValidator is a distinct typed.
	*chasm.LinkValidator
}

func newLinkValidator(
	maxLinksPerRequest dynamicconfig.IntPropertyFnWithNamespaceFilter,
	maxLinksPerComponent dynamicconfig.IntPropertyFnWithNamespaceFilter,
	linkMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *linkValidator {
	return &linkValidator{
		chasm.NewLinkValidator(
			"a nexus operation",
			maxLinksPerRequest,
			maxLinksPerComponent,
			linkMaxSize,
		),
	}
}
