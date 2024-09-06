// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package authorization

import (
	"context"

	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	defaultAuthorizer struct {
		logger log.Logger
	}
)

var _ Authorizer = (*defaultAuthorizer)(nil)

// NewDefaultAuthorizer creates a default authorizer
func NewDefaultAuthorizer(logger log.Logger) Authorizer {
	logger.Debug("Creating new default authorizer")
	return &defaultAuthorizer{logger: logger}
}

var resultAllow = Result{Decision: DecisionAllow}
var resultDeny = Result{Decision: DecisionDeny}

// Authorize determines if an API call by given claims should be allowed or denied.
// Rules:
//
//	Health check APIs are allowed to everyone.
//	System Admin is allowed to access all APIs on all namespaces and cluster-level.
//	System Writer is allowed to access non admin APIs on all namespaces and cluster-level.
//	System Reader is allowed to access readonly APIs on all namespaces and cluster-level.
//	Namespace Admin is allowed to access all APIs on their namespaces.
//	Namespace Writer is allowed to access non admin APIs on their namespaces.
//	Namespace Reader is allowed to access non admin readonly APIs on their namespaces.
func (a *defaultAuthorizer) Authorize(ctx context.Context, claims *Claims, target *CallTarget) (Result, error) {
	a.logger.Debug("Authorizing request", tag.NewAnyTag("claims", claims), tag.NewAnyTag("target", target))

	// APIs that are essentially read-only health checks with no sensitive information are
	// always allowed
	if IsHealthCheckAPI(target.APIName) {
		a.logger.Debug("Health check API detected, allowing request", tag.NewAnyTag("apiName", target.APIName))
		return resultAllow, nil
	}
	if claims == nil {
		a.logger.Debug("No claims provided, denying request")
		return resultDeny, nil
	}

	metadata := api.GetMethodMetadata(target.APIName)
	a.logger.Debug("Retrieved method metadata", tag.NewAnyTag("metadata", metadata))

	var hasRole Role
	switch metadata.Scope {
	case api.ScopeCluster:
		hasRole = claims.System
		a.logger.Debug("Cluster scope detected", tag.NewAnyTag("hasRole", hasRole))
	case api.ScopeNamespace:
		// Note: system-level claims apply across all namespaces.
		// Note: if claims.Namespace is nil or target.Namespace is not found, the lookup will return zero.
		hasRole = claims.System | claims.Namespaces[target.Namespace]
		a.logger.Debug("Namespace scope detected", tag.NewAnyTag("hasRole", hasRole))
	default:
		a.logger.Debug("Unknown scope detected, denying request", tag.NewAnyTag("scope", metadata.Scope))
		return resultDeny, nil
	}

	requiredRole := getRequiredRole(metadata.Access)
	a.logger.Debug("Required role determined", tag.NewAnyTag("requiredRole", requiredRole))

	if hasRole >= requiredRole {
		a.logger.Debug("Role check passed, allowing request", tag.NewAnyTag("hasRole", hasRole), tag.NewAnyTag("requiredRole", requiredRole))
		return resultAllow, nil
	}
	a.logger.Debug("Role check failed, denying request", tag.NewAnyTag("hasRole", hasRole), tag.NewAnyTag("requiredRole", requiredRole))
	return resultDeny, nil
}

// Convert from api.Access to Role
func getRequiredRole(access api.Access) Role {
	switch access {
	case api.AccessReadOnly:
		return RoleReader
	case api.AccessWrite:
		return RoleWriter
	default:
		return RoleAdmin
	}
}
