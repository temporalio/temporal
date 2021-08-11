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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination authorizer_mock.go

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
