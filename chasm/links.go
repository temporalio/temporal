package chasm

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	commonlinks "go.temporal.io/server/common/links"
)

// LinkValidator validates links a caller attaches to a CHASM component, on the request that starts
// the execution and on any subsequent on-conflict attach. It enforces both per-request limits
// (count + size + variant shape) and a per-component cumulative cap across those calls.
//
// The limits are supplied by the owning library rather than read here, so each component type can
// source them from its own dynamic config settings.
type LinkValidator struct {
	// componentName names the component in error messages, as a noun phrase including its article:
	// "an activity", "a nexus operation".
	componentName        string
	maxLinksPerRequest   dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxLinksPerComponent dynamicconfig.IntPropertyFnWithNamespaceFilter
	linkMaxSize          dynamicconfig.IntPropertyFnWithNamespaceFilter
}

// NewLinkValidator returns a LinkValidator for a component type named by componentName, using the
// given per-namespace limits.
func NewLinkValidator(
	componentName string,
	maxLinksPerRequest dynamicconfig.IntPropertyFnWithNamespaceFilter,
	maxLinksPerComponent dynamicconfig.IntPropertyFnWithNamespaceFilter,
	linkMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *LinkValidator {
	return &LinkValidator{
		componentName:        componentName,
		maxLinksPerRequest:   maxLinksPerRequest,
		maxLinksPerComponent: maxLinksPerComponent,
		linkMaxSize:          linkMaxSize,
	}
}

// ValidateRequest checks count, per-link size, and variant shape for the links
// on a single incoming request.
func (v *LinkValidator) ValidateRequest(namespaceName string, links []*commonpb.Link) error {
	return commonlinks.Validate(links, v.maxLinksPerRequest(namespaceName), v.linkMaxSize(namespaceName))
}

// ValidateComponentTotal checks that adding addingCount links to a component
// already holding existingCount links would not exceed the per-component cap.
func (v *LinkValidator) ValidateComponentTotal(namespaceName string, existingCount, addingCount int) error {
	maxLinks := v.maxLinksPerComponent(namespaceName)
	if existingCount+addingCount > maxLinks {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d links to %s (%d links already attached)",
			maxLinks,
			v.componentName,
			existingCount,
		)
	}
	return nil
}
