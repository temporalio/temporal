package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
)

// linkValidator validates links attached to standalone activity executions.
type linkValidator struct {
	// Wrapped so that activity.linkValidator is a distinct, injectable type.
	*chasm.LinkValidator
}

func newLinkValidator(
	maxLinksPerRequest dynamicconfig.IntPropertyFnWithNamespaceFilter,
	maxLinksPerComponent dynamicconfig.IntPropertyFnWithNamespaceFilter,
	linkMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *linkValidator {
	return &linkValidator{
		chasm.NewLinkValidator(
			"an activity",
			maxLinksPerRequest,
			maxLinksPerComponent,
			linkMaxSize,
		),
	}
}
