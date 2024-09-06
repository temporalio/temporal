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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	// DecisionDeny means auth decision is deny
	DecisionDeny Decision = iota + 1
	// DecisionAllow means auth decision is allow
	DecisionAllow
)

// CallTarget contains information for Authorizer to make a decision.
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

// Authorizer is an interface for implementing authorization logic
type Authorizer interface {
	Authorize(ctx context.Context, caller *Claims, target *CallTarget) (Result, error)
}

type hasNamespace interface {
	GetNamespace() string
}

func GetAuthorizerFromConfig(config *config.Authorization, logger log.Logger) (Authorizer, error) {
	logger.Debug("Getting authorizer from config", tag.NewAnyTag("config", config))

	switch strings.ToLower(config.Authorizer) {
	case "":
		logger.Debug("No authorizer specified, using NoopAuthorizer")
		return NewNoopAuthorizer(), nil
	case "default":
		logger.Debug("Default authorizer specified, using DefaultAuthorizer")
		return NewDefaultAuthorizer(logger), nil
	}
	err := fmt.Errorf("unknown authorizer: %s", config.Authorizer)
	logger.Error("Unknown authorizer", tag.Error(err))
	return nil, err
}

func IsNoopAuthorizer(authorizer Authorizer, logger log.Logger) bool {
	_, ok := authorizer.(*noopAuthorizer)
	logger.Debug("Checking if authorizer is NoopAuthorizer", tag.NewAnyTag("isNoop", ok))
	return ok
}
