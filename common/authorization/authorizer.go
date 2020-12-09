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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination authority_mock.go -self_package go.temporal.io/server/common/authorization

package authorization

import "context"

const (
	// DecisionDeny means auth decision is deny
	DecisionDeny Decision = iota + 1
	// DecisionAllow means auth decision is allow
	DecisionAllow
)

// @@@SNIPSTART temporal-common-authorization-authorizer-calltarget
// CallTarget is input for authorizer to make decision.
// It can be extended in future if required auth on resources like WorkflowType and TaskQueue
type CallTarget struct {
	// The `APIName` field must be a string of the API name.
	// Example: "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution".
	APIName   string
	// If a Namespace is not being targeted, `Namespace` can be set to an empty string.
	Namespace string
}
// @@@SNIPEND

type (
	// Result is result from authority.
	Result struct {
		Decision Decision
	}
)

// @@@SNIPSTART temporal-common-authorization-authorizer-interface
// Authorizer is an interface for authorization
type Authorizer interface {
	Authorize(ctx context.Context, caller *Claims, target *CallTarget) (Result, error)
}
// @@@SNIPEND

type requestWithNamespace interface {
	GetNamespace() string
}
