//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination authorizer_mock.go

package authorization

import (
	"context"
	"fmt"
	"strings"

	"go.temporal.io/server/common/config"
)

const (
	// DecisionDeny means auth decision is deny
	DecisionDeny Decision = iota + 1
	// DecisionAllow means auth decision is allow
	DecisionAllow
)

// @@@SNIPSTART temporal-common-authorization-authorizer-calltarget
// CallTarget is contains information for Authorizer to make a decision.
// It can be extended to include resources like WorkflowType and TaskQueue
type CallTarget struct {
	// APIName must be the full API function name.
	// Example: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution".
	APIName string
	// If a Namespace is not being targeted this be set to an empty string.
	Namespace string
	// The nexus endpoint name being targeted (if any).
	NexusEndpointName string
	// Request contains a deserialized copy of the API request object
	Request interface{}
}

// @@@SNIPEND

type (
	// Result is result from authority.
	Result struct {
		Decision Decision
		// Reason may contain a message explaining the value of the Decision field.
		Reason string
	}

	// Decision is enum type for auth decision
	Decision int
)

// @@@SNIPSTART temporal-common-authorization-authorizer-interface
// Authorizer is an interface for implementing authorization logic
type Authorizer interface {
	Authorize(ctx context.Context, caller *Claims, target *CallTarget) (Result, error)
}

// @@@SNIPEND

type hasNamespace interface {
	GetNamespace() string
}

func GetAuthorizerFromConfig(config *config.Authorization) (Authorizer, error) {

	switch strings.ToLower(config.Authorizer) {
	case "":
		return NewNoopAuthorizer(), nil
	case "default":
		return NewDefaultAuthorizer(), nil
	}
	return nil, fmt.Errorf("unknown authorizer: %s", config.Authorizer)
}

func IsNoopAuthorizer(authorizer Authorizer) bool {
	_, ok := authorizer.(*noopAuthorizer)
	return ok
}
