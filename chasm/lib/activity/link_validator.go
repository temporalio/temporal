package activity

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	commonlinks "go.temporal.io/server/common/links"
)

// linkValidator validates links attached to standalone activity executions.
// It enforces both per-request limits (count + size + variant shape) and a
// per-execution cumulative cap across start/attach calls.
type linkValidator struct {
	maxLinksPerRequest   dynamicconfig.IntPropertyFnWithNamespaceFilter
	maxLinksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter
	linkMaxSize          dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func newLinkValidator(
	maxLinksPerRequest dynamicconfig.IntPropertyFnWithNamespaceFilter,
	maxLinksPerExecution dynamicconfig.IntPropertyFnWithNamespaceFilter,
	linkMaxSize dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *linkValidator {
	return &linkValidator{
		maxLinksPerRequest:   maxLinksPerRequest,
		maxLinksPerExecution: maxLinksPerExecution,
		linkMaxSize:          linkMaxSize,
	}
}

// ValidateRequest checks count, per-link size, and variant shape for the links
// on a single incoming request.
func (v *linkValidator) ValidateRequest(_ context.Context, namespaceName string, links []*commonpb.Link) error {
	return commonlinks.Validate(links, v.maxLinksPerRequest(namespaceName), v.linkMaxSize(namespaceName))
}

// ValidateExecutionTotal checks that adding addingCount links to an execution
// already holding existingCount links would not exceed the per-execution cap.
func (v *linkValidator) ValidateExecutionTotal(namespaceName string, existingCount, addingCount int) error {
	maxLinks := v.maxLinksPerExecution(namespaceName)
	if existingCount+addingCount > maxLinks {
		return serviceerror.NewFailedPreconditionf(
			"cannot attach more than %d links to an activity (%d links already attached)",
			maxLinks,
			existingCount,
		)
	}
	return nil
}
