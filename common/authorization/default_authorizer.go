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
	"strings"
)

type (
	defaultAuthorizer struct {
	}
)

const (
	operatorServicePrefix = "/temporal.api.operatorservice.v1.OperatorService/"
	adminServicePrefix    = "/temporal.server.api.adminservice.v1.AdminService/"
)

var _ Authorizer = (*defaultAuthorizer)(nil)

// NewDefaultAuthorizer creates a default authorizer
func NewDefaultAuthorizer() Authorizer {
	return &defaultAuthorizer{}
}

var resultAllow = Result{Decision: DecisionAllow}
var resultDeny = Result{Decision: DecisionDeny}

// Authorize determines if an API call by given claims should be allowed or denied.
// Rules:
//
//	Health check APIs are allowed to everyone.
//	System Admin is allowed to access all APIs on all namespaces.
//	System Writer is allowed to access non admin APIs on all namespaces.
//	System Reader is allowed to access readonly APIs on all namespaces.
//	Namespace Admin is allowed to access all APIs on their namespaces.
//	Namespace Writer is allowed to access non admin APIs on their namespaces.
//	Namespace Reader is allowed to access non admin readonly APIs on their namespaces.
func (a *defaultAuthorizer) Authorize(_ context.Context, claims *Claims, target *CallTarget) (Result, error) {
	// APIs that are essentially read-only health checks with no sensitive information are
	// always allowed
	if IsHealthCheckAPI(target.APIName) {
		return resultAllow, nil
	}

	if claims == nil {
		return resultDeny, nil
	}
	// System Admin is allowed for everything
	if claims.System >= RoleAdmin {
		return resultAllow, nil
	}

	// admin service means admin / operator service
	isAdminService := strings.HasPrefix(target.APIName, adminServicePrefix) || strings.HasPrefix(target.APIName, operatorServicePrefix)

	// System Writer is allowed for non admin service APIs
	if claims.System >= RoleWriter && !isAdminService {
		return resultAllow, nil
	}

	api := ApiName(target.APIName)
	readOnlyNamespaceAPI := IsReadOnlyNamespaceAPI(api)
	readOnlyGlobalAPI := IsReadOnlyGlobalAPI(api)
	// System Reader is allowed for all read only APIs
	if claims.System >= RoleReader && (readOnlyNamespaceAPI || readOnlyGlobalAPI) {
		return resultAllow, nil
	}

	// Below are for non system roles.
	role, found := claims.Namespaces[target.Namespace]
	if !found || role == RoleUndefined {
		return resultDeny, nil
	}

	if isAdminService {
		// for admin service APIs, only RoleAdmin of given namespace can access
		if role >= RoleAdmin {
			return resultAllow, nil
		}
	} else {
		// for non admin service APIs
		if role >= RoleWriter {
			return resultAllow, nil
		}
		if role >= RoleReader && readOnlyNamespaceAPI {
			return resultAllow, nil
		}
	}

	return resultDeny, nil
}

func ApiName(api string) string {
	index := strings.LastIndex(api, "/")
	if index > -1 {
		return api[index+1:]
	}
	return api
}
