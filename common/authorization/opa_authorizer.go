// The MIT License
//
// Copyright (c) 2023 Manetu Inc.  All rights reserved.
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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.temporal.io/server/common/log"

	"github.com/open-policy-agent/opa/ast"
	"github.com/open-policy-agent/opa/rego"
	"go.temporal.io/server/common/authorization/policies"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log/tag"
)

type opaAuthorizer struct {
	compiler *ast.Compiler
	logger   log.Logger
	trace    bool
}

func importFile(modules map[string]string, spec string) error {
	pattern := strings.TrimPrefix(spec, "file:")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, path := range files {
		policy, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		modules[path] = string(policy)
	}

	return nil
}

func importInternal(modules map[string]string, id string) error {
	name := strings.TrimPrefix(id, "internal:")
	policy, err := policies.Content.ReadFile(name + ".rego")
	if err != nil {
		return err
	}
	modules[id] = string(policy)
	return nil
}

func importInline(modules map[string]string, policy string, idx int) error {
	modules[fmt.Sprintf("inline/%d.rego", idx)] = policy
	return nil
}

var defaultPolicies = []string{"internal:default"}

// NewOpaAuthorizer creates a Open Policy Agent based authorizer
func NewOpaAuthorizer(config *config.Authorization, logger log.Logger) Authorizer {
	var policyDefs = defaultPolicies

	if len(config.Policies) > 0 {
		policyDefs = config.Policies
	}

	modules := map[string]string{}
	for idx, policy := range policyDefs {
		switch {
		case strings.HasPrefix(policy, "file:"):
			err := importFile(modules, policy)
			if err != nil {
				logger.Error("Could not load external policy file", tag.NewStringTag("policy", policy), tag.Error(err))
				return nil
			}
		case strings.HasPrefix(policy, "internal:"):
			err := importInternal(modules, policy)
			if err != nil {
				logger.Error("Could not load internal policy", tag.NewStringTag("policy", policy), tag.Error(err))
				return nil
			}
		case strings.HasPrefix(policy, "package"):
			err := importInline(modules, policy, idx)
			if err != nil {
				logger.Error("Could not load inline policy", tag.NewStringTag("policy", policy), tag.Error(err))
				return nil
			}
		default:
			logger.Error("Could not parse policy", tag.NewStringTag("policy", policy))
			return nil
		}
	}

	compiler, err := ast.CompileModules(modules)
	if err != nil {
		logger.Error("REGO compilation failed", tag.Error(err))
		return nil
	}

	return &opaAuthorizer{
		compiler: compiler,
		logger:   logger,
		trace:    false,
	}
}

func (a *opaAuthorizer) Authorize(ctx context.Context, claims *Claims, target *CallTarget) (Result, error) {
	var decision = DecisionDeny

	input := map[string]interface{}{
		"claims": claims,
		"target": target,
	}
	query := rego.New(
		rego.Query("allow = data.temporal.authz.allow"),
		rego.Compiler(a.compiler),
		rego.Input(input),
		rego.Trace(a.trace),
	)

	results, err := query.Eval(ctx)
	if err != nil {
		a.logger.Error("REGO evaluation failed", tag.Error(err))
		return Result{Decision: DecisionDeny}, err
	}

	if a.trace {
		traceBuffer := new(strings.Builder)
		rego.PrintTrace(traceBuffer, query)
		traceResult := traceBuffer.String()
		a.logger.Debug("REGO trace", tag.NewStringTag("trace", traceResult))
	}
	if results[0].Bindings["allow"].(bool) {
		decision = DecisionAllow
	}

	return Result{Decision: decision}, nil
}
