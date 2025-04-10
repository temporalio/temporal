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
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	codeCoverageExtension = ".cover.out"
	retriesFlag           = "--retries="
	coverProfileFlag      = "-coverprofile="
	junitReportFlag       = "--junitfile="
)

type attempt struct {
	runner           *runner
	number           int
	exitErr          *exec.ExitError
	junitReport      *junitReport
	coverProfilePath string
}

func (a *attempt) run(ctx context.Context, args []string) (string, error) {
	for i, arg := range args {
		if strings.HasPrefix(arg, coverProfileFlag) {
			args[i] = coverProfileFlag + a.coverProfilePath
		} else if strings.HasPrefix(arg, junitReportFlag) {
			args[i] = junitReportFlag + a.junitReport.path
		}
	}
	log.Printf("starting test attempt #%d: %v %v",
		a.number, a.runner.gotestsumExecutable, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, a.runner.gotestsumExecutable, args...)
	var output strings.Builder
	cmd.Stdout = io.MultiWriter(os.Stdout, &output)
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return output.String(), err
	}
	return "", nil
}

type runner struct {
	gotestsumExecutable string
	junitOutputPath     string
	coverProfilePath    string
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
	for _, arg := range args {
		if strings.HasPrefix(arg, retriesFlag) {
			var err error
			r.retries, err = strconv.Atoi(strings.Split(arg, "=")[1])
			if err != nil {
				return nil, fmt.Errorf("invalid argument %q: %w", retriesFlag, err)
			}
			if r.retries == 0 {
				return nil, fmt.Errorf("invalid argument %q: must be greater than zero", retriesFlag)
			}
			continue // this is a `testrunner` only arg and not passed through
		}

		if strings.HasPrefix(arg, coverProfileFlag) {
			r.coverProfilePath = strings.Split(arg, "=")[1]
		} else if strings.HasPrefix(arg, junitReportFlag) {
			// --junitfile is used by gotestsum
			r.junitOutputPath = strings.Split(arg, "=")[1]
		}

		sanitizedArgs = append(sanitizedArgs, arg)
	}
	if r.junitOutputPath == "" {
		return nil, fmt.Errorf("missing required argument %q", junitReportFlag)
	}
	if r.coverProfilePath == "" {
		return nil, fmt.Errorf("missing required argument %q", coverProfileFlag)
	}
	if r.retries == 0 {
		return nil, fmt.Errorf("missing required argument %q", retriesFlag)
	}
	return sanitizedArgs, nil
}

func (r *runner) newAttempt() *attempt {
	a := &attempt{
		runner: r,
		number: len(r.attempts) + 1,
		coverProfilePath: fmt.Sprintf(
			"%v_%v%v",
			strings.TrimSuffix(r.coverProfilePath, codeCoverageExtension),
			len(r.attempts),
			codeCoverageExtension),
		junitReport: &junitReport{
			path: filepath.Join(os.TempDir(), fmt.Sprintf("temporalio-temporal-%s-junit.xml", uuid.NewString())),
		},
	}
	r.attempts = append(r.attempts, a)
	return a
}

func (r *runner) allReports() []*junitReport {
	var reports []*junitReport
	for _, a := range r.attempts {
		reports = append(reports, a.junitReport)
	}
	return reports
}

func Main() {
	log.SetPrefix("[testrunner] ")
	ctx := context.Background()
	if len(os.Args) < 2 {
		log.Fatalf("expected at least 2 arguments")
	}
	r := newRunner(os.Args[1])
	args, err := r.sanitizeAndParseArgs(os.Args[2:])
	if err != nil {
		log.Fatalf("failed to parse command line options: %v", err)
	}

	var currentAttempt *attempt
	for retry := 0; retry <= r.retries; retry++ {
		currentAttempt = r.newAttempt()

		// Run tests.
		stdout, err := currentAttempt.run(ctx, args)
		if err != nil && !errors.As(err, &currentAttempt.exitErr) {
			log.Fatalf("test run failed with an unexpected error: %v", err)
		}

		stacktrace, timedoutTests := parseTestTimeouts(stdout)
		if len(timedoutTests) > 0 {
			// Run timed out and was aborted.
			// Update JUnit XML output for timed out tests since none will have been generated.
			currentAttempt.junitReport = generateForTimedoutTests(timedoutTests)
			log.Print(stacktrace)

			// Don't retry.
			break
		}

		// All tests were run, parse JUnit XML output.
		if err = currentAttempt.junitReport.read(); err != nil {
			log.Fatal(err)
		}

		// If the run completely successfull, no need to retry.
		if currentAttempt.exitErr == nil {
			break
		}

		// Sanity check: make sure failures are reported when the run failed.
		failures := currentAttempt.junitReport.collectTestCaseFailures()
		if len(failures) == 0 {
			log.Fatalf("tests failed but no failures have been detected, not rerunning tests")
		}

		// Rerun all tests from previous attempt if there's more than 10 failures in a single suite.
		if len(failures) > 10 && retry < r.retries {
			log.Printf(
				"number of failures exceeds configured threshold (%d/%d) for narrowing down tests to retry, retrying with previous attempt's args",
				len(failures), 10)
			continue
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

	// Merge reports from all attempts and write the final JUnit report.
	mergedReport, err := mergeReports(r.allReports())
	if err != nil {
		log.Fatal(err)
	}
	mergedReport.path = r.junitOutputPath
	if err = mergedReport.write(); err != nil {
		log.Fatal(err)
	}

	// Exit with the exit code of the last attempt.
	if currentAttempt.exitErr != nil {
		log.Printf("exiting with failure after running %d attempt(s)", len(r.attempts))
		os.Exit(currentAttempt.exitErr.ExitCode())
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
