// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package testrunner

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"iter"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/jstemmer/go-junit-report/v2/junit"
)

type node struct {
	children map[string]node
}

func (n node) visitor(path ...string) func(yield func(string, node) bool) {
	return func(yield func(string, node) bool) {
		for name, child := range n.children {
			path := append(path, name)
			if !yield(strings.Join(path, "/"), child) {
				return
			}
			child.visitor(path...)(yield)
		}
	}
}

func (n node) walk() iter.Seq2[string, node] {
	return n.visitor()
}

type attempt struct {
	runner       *runner
	number       int
	junitXmlPath string
	exitErr      *exec.ExitError
	suites       junit.Testsuites
}

func (a *attempt) run(ctx context.Context, args []string) error {
	args = append([]string{"--junitfile", a.junitXmlPath}, args...)
	log.Printf("starting test attempt %d with args: %v", a.number, args)
	cmd := exec.CommandContext(ctx, a.runner.gotestsumExecutable, args...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

func (a *attempt) recordResult(exitErr *exec.ExitError) error {
	var suites junit.Testsuites

	f, err := os.Open(a.junitXmlPath)
	if err != nil {
		return err
	}
	defer f.Close()

	decoder := xml.NewDecoder(f)
	if err := decoder.Decode(&suites); err != nil {
		return err
	}
	a.exitErr = exitErr
	a.suites = suites
	return nil
}

func (a attempt) failures() []string {
	root := node{children: make(map[string]node)}

	for _, suite := range a.suites.Suites {
		if suite.Failures == 0 {
			continue
		}
		for _, tc := range suite.Testcases {
			if tc.Failure == nil {
				continue
			}
			n := root
			for _, part := range strings.Split(tc.Name, "/") {
				child, ok := n.children[part]
				if !ok {
					child = node{children: make(map[string]node)}
					n.children[part] = child
				}
				n = child
			}
		}
	}

	leafFailures := make([]string, 0)

	// Walk the tree and find all leaf failures. The way Go test failures are reported in junit is that there's a
	// test case per suite and per test in that suite. Filter out any nodes that have children to find the most
	// specific failures to rerun.
	for path, n := range root.walk() {
		if len(n.children) > 0 {
			continue
		}
		leafFailures = append(leafFailures, path)
	}

	return leafFailures
}

type runner struct {
	gotestsumExecutable string
	junitOutputPath     string
	attempts            []*attempt
	retries             int
}

func newRunner(gotestsumExecutable string) *runner {
	return &runner{
		gotestsumExecutable: gotestsumExecutable,
		attempts:            make([]*attempt, 0),
	}
}

func (r *runner) sanitizeAndParseArgs(args []string) ([]string, error) {
	var sanitizedArgs []string
	type action struct {
		f   func(string) error
		err string
	}
	var next *action
	for i, arg := range args {
		if next != nil {
			if err := next.f(arg); err != nil {
				return nil, err
			}
			next = nil
			continue
		} else if arg == "-retries" {
			next = &action{
				f: func(arg string) error {
					var err error
					r.retries, err = strconv.Atoi(arg)
					return err
				},
				err: "got -retries flag with no value",
			}
			continue
		} else if strings.HasPrefix(arg, "-retries=") {
			var err error
			r.retries, err = strconv.Atoi(arg[len("-retries="):])
			if err != nil {
				return nil, err
			}
			continue
		} else if arg == "--junitfile" {
			// --junitfile is used by gotestsum
			next = &action{
				f: func(arg string) error {
					r.junitOutputPath = arg
					return nil
				},
				err: "got --junitfile flag with no value",
			}
			continue
		} else if strings.HasPrefix(arg, "--junitfile=") {
			// --junitfile is used by gotestsum
			r.junitOutputPath = arg[len("--junitfile="):]
			continue
		} else if arg == "--" {
			// Forward all arguments from -- on.
			sanitizedArgs = append(sanitizedArgs, args[i:]...)
			break
		}
		sanitizedArgs = append(sanitizedArgs, arg)
	}
	if next != nil {
		return nil, fmt.Errorf("incomplete command line arguments: %s", next.err)
	}
	if r.junitOutputPath == "" {
		return nil, fmt.Errorf("missing required argument --junitfile")
	}
	return sanitizedArgs, nil
}

func (r *runner) newAttempt() *attempt {
	f := filepath.Join(os.TempDir(), fmt.Sprintf("temporalio-temporal-%s-junit.xml", uuid.NewString()))
	a := &attempt{
		runner:       r,
		number:       len(r.attempts) + 1,
		junitXmlPath: f,
	}
	r.attempts = append(r.attempts, a)
	return a
}

func (r *runner) combineAttempts() junit.Testsuites {
	var combined junit.Testsuites

	for i, attempt := range r.attempts {
		combined.Tests += attempt.suites.Tests
		combined.Errors += attempt.suites.Errors
		combined.Failures += attempt.suites.Failures
		combined.Skipped += attempt.suites.Skipped
		combined.Disabled += attempt.suites.Disabled
		combined.Time += attempt.suites.Time

		if i == 0 {
			combined.XMLName = attempt.suites.XMLName
			combined.Name = attempt.suites.Name
			combined.Suites = attempt.suites.Suites
			continue
		}

		// Just a sanity check for this tool since it's new and we want to make sure we actually rerun what we
		// expect.
		if attempt.suites.Tests != r.attempts[i-1].suites.Failures {
			log.Fatalf("expected a rerun of all failures from the previous attempt, got (%d/%d)", attempt.suites.Tests, r.attempts[i-1].suites.Failures)
		}

		for _, suite := range attempt.suites.Suites {
			cpy := suite
			cpy.Name += fmt.Sprintf(" (retry %d)", i)
			cpy.Testcases = make([]junit.Testcase, 0, len(suite.Testcases))
			for _, test := range suite.Testcases {
				tcpy := test
				tcpy.Name += fmt.Sprintf(" (retry %d)", i)
				cpy.Testcases = append(cpy.Testcases, tcpy)
			}
			combined.Suites = append(combined.Suites, cpy)
		}
	}

	return combined
}

func Main() {
	ctx := context.Background()
	if len(os.Args) < 2 {
		log.Fatalf("expected at least 2 arguments")
	}
	r := newRunner(os.Args[1])
	args, err := r.sanitizeAndParseArgs(os.Args[2:])
	if err != nil {
		log.Fatalf("failed to parse command line options: %v", err)
	}

	for retry := 0; retry <= r.retries; retry++ {
		var exitErr *exec.ExitError
		a := r.newAttempt()
		err := a.run(ctx, args)
		if err != nil && !errors.As(err, &exitErr) {
			log.Fatalf("test run failed with an unexpected error: %v", err)
		}
		if err := a.recordResult(exitErr); err != nil {
			log.Fatalf("failed to record run result: %v", err)
		}
		if exitErr == nil {
			break
		}
		failures := r.attempts[retry].failures()
		if len(failures) == 0 {
			log.Fatalf("tests failed but no failures have been detected, not rerunning tests")
		}
		// Don't rerun if there's more than 10 failures in a single suite.
		if len(failures) > 10 && retry < r.retries {
			log.Printf("will not rerun tests, number of failures exceeds configured threshold (%d/%d)", len(failures), 10)
			break
		}
		args = stripRunFromArgs(args)
		for i, failure := range failures {
			failures[i] = goTestNameToRunFlagRegexp(failure)
		}
		failureArg := strings.Join(failures, "|")
		// -args has special semantics in Go.
		argsIdx := slices.Index(args, "-args")
		if argsIdx == -1 {
			args = append(args, "-run", failureArg)
		} else {
			args = slices.Insert(args, argsIdx, "-run", failureArg)
		}
	}

	combinedReport := r.combineAttempts()
	f, err := os.Create(r.junitOutputPath)
	if err != nil {
		log.Fatalf("failed to create junit output file: %v", err)
	}
	defer f.Close()
	enc := xml.NewEncoder(f)
	enc.Indent("", "    ")
	if err := enc.Encode(combinedReport); err != nil {
		log.Fatalf("failed to encode junit output: %v", err)
	}

	lastAttempt := r.attempts[len(r.attempts)-1]
	if lastAttempt.exitErr != nil {
		log.Printf("exiting with failure after running %d attempt(s)", len(r.attempts))
		os.Exit(lastAttempt.exitErr.ExitCode())
	}
}

func stripRunFromArgs(args []string) (argsNoRun []string) {
	var skipNext bool
	for _, arg := range args {
		if skipNext {
			skipNext = false
			continue
		} else if arg == "-run" {
			skipNext = true
			continue
		} else if strings.HasPrefix(arg, "-run=") {
			continue
		}
		argsNoRun = append(argsNoRun, arg)
	}
	return
}

func goTestNameToRunFlagRegexp(test string) string {
	parts := strings.Split(test, "/")
	var sb strings.Builder
	for i, p := range parts {
		if i > 0 {
			sb.WriteByte('/')
		}
		sb.WriteByte('^')
		sb.WriteString(regexp.QuoteMeta(p))
		sb.WriteByte('$')
	}
	return sb.String()
}
