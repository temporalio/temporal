// Copyright (c) 2019 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination authority_mock.go -self_package github.com/uber/cadence/common/authorization

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
		Actor      string
		APIName    string
		DomainName string
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
