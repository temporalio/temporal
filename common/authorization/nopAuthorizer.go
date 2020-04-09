package authorization

import "context"

type nopAuthority struct{}

// NewNopAuthorizer creates a no-op authority
func NewNopAuthorizer() Authorizer {
	return &nopAuthority{}
}

func (a *nopAuthority) Authorize(
	ctx context.Context,
	attributes *Attributes,
) (Result, error) {
	return Result{Decision: DecisionAllow}, nil
}
