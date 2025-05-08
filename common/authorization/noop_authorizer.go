package authorization

import "context"

type noopAuthorizer struct{}

// NewNoopAuthorizer creates a no-op authorizer
func NewNoopAuthorizer() Authorizer {
	return &noopAuthorizer{}
}

func (a *noopAuthorizer) Authorize(_ context.Context, _ *Claims, _ *CallTarget) (Result, error) {
	return Result{Decision: DecisionAllow}, nil
}
