//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination authority_mock.go -self_package github.com/temporalio/temporal/common/authorization

package authorization

import "context"

const (
	// DecisionDeny means auth decision is deny
	DecisionDeny Decision = iota + 1
	// DecisionAllow means auth decision is allow
	DecisionAllow
)

type (
	// Attributes is input for authority to make decision.
	// It can be extended in future if required auth on resources like WorkflowType and TaskList
	Attributes struct {
		Actor     string
		APIName   string
		Namespace string
	}

	// Result is result from authority.
	Result struct {
		Decision Decision
	}

	// Decision is enum type for auth decision
	Decision int
)

// Authorizer is an interface for authorization
type Authorizer interface {
	Authorize(ctx context.Context, attributes *Attributes) (Result, error)
}
