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

package tests

import (
	"context"
	"strings"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// AcquireShardFunctionalSuite is the base test suite for testing acquire shard.
type AcquireShardFunctionalSuite struct {
	FunctionalTestBase
	logRecorder *logRecorder
	logs        chan logRecord
}

// SetupSuite sets up the test suite by setting the log recorder.
func (s *AcquireShardFunctionalSuite) SetupSuite() {
	s.logs = make(chan logRecord, 100)
	s.logRecorder = newLogRecorder(s.logs)
	s.Logger = s.logRecorder
}

// TearDownSuite tears down the test suite by shutting down the test cluster after a short delay.
func (s *AcquireShardFunctionalSuite) TearDownSuite() {
	// we need to wait for all components to start before we can safely tear down
	time.Sleep(time.Second * 5)
	s.tearDownSuite()
}

// newLogRecorder creates a new log recorder. It records all the logs to the given channel.
func newLogRecorder(logs chan logRecord) *logRecorder {
	return &logRecorder{
		Logger: log.NewNoopLogger(),
		logs:   logs,
	}
}

// logRecord represents a call to Info or Error.
type logRecord struct {
	msg  string
	tags []tag.Tag
}

// logRecorder is used to record log messages
type logRecorder struct {
	log.Logger
	logs chan logRecord
}

// Info records the info message
func (r *logRecorder) Info(msg string, tags ...tag.Tag) {
	r.record(msg, tags...)
}

// Error records the error message
func (r *logRecorder) Error(msg string, tags ...tag.Tag) {
	r.record(msg, tags...)
}

// Fatal ignores the fatal call
func (r *logRecorder) Fatal(string, ...tag.Tag) {
	// Fatal errors sometimes happen for other components we instantiate and tear down during this test.
}

// record sends a logRecord to the logs channel. If there is no active receiver, the logRecord will be dropped.
func (r *logRecorder) record(msg string, tags ...tag.Tag) {
	select {
	case r.logs <- logRecord{
		msg:  msg,
		tags: tags,
	}:
	default:
	}
}

// OwnershipLostErrorSuite is the test suite for testing what happens when acquire shard returns an ownership lost
// error.
type OwnershipLostErrorSuite struct {
	AcquireShardFunctionalSuite
}

// SetupSuite reads the shard ownership lost error fault injection config from the testdata folder.
func (s *OwnershipLostErrorSuite) SetupSuite() {
	s.AcquireShardFunctionalSuite.SetupSuite()
	s.setupSuite("testdata/acquire_shard_ownership_lost_error.yaml")
}

// TestDoesNotRetry verifies that we do not retry acquiring the shard when we get an ownership lost error.
func (s *OwnershipLostErrorSuite) TestDoesNotRetry() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	numAttempts := 0
	for {
		select {
		case record := <-s.logs:
			msg := strings.ToLower(record.msg)
			if strings.Contains(msg, "error acquiring shard") {
				numAttempts++
				taggedAsNonRetryable := false
				for _, tg := range record.tags {
					if tg == tag.IsRetryable(false) {
						taggedAsNonRetryable = true
					}
				}
				s.True(taggedAsNonRetryable, "logged error should be tagged as non-retryable")
			} else if strings.Contains(msg, "acquired shard") {
				s.FailNow("should not acquire shard")
			} else if strings.Contains(msg, "couldn't acquire shard") {
				s.Equal(1, numAttempts, "should fail after one attempt")
				return
			}
		case <-ctx.Done():
			s.FailNow("timed out waiting for shard update error")
		}
	}
}

// DeadlineExceededErrorSuite is the test suite for testing what happens when acquire shard returns a deadline exceeded.
type DeadlineExceededErrorSuite struct {
	AcquireShardFunctionalSuite
}

// SetupSuite reads the deadline exceeded error targeted fault injection config from the test data folder.
func (s *DeadlineExceededErrorSuite) SetupSuite() {
	s.AcquireShardFunctionalSuite.SetupSuite()
	s.setupSuite("testdata/acquire_shard_deadline_exceeded_error.yaml")
}

// TestDoesRetry verifies that we do retry acquiring the shard when we get a deadline exceeded error because that should
// be considered a transient error.
func (s *DeadlineExceededErrorSuite) TestDoesRetry() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	for numAttempts := 0; numAttempts < 2; {
		select {
		case record := <-s.logs:
			msg := strings.ToLower(record.msg)
			if strings.Contains(msg, "error acquiring shard") {
				numAttempts++
				taggedAsRetryable := false
				for _, tg := range record.tags {
					if tg == tag.IsRetryable(true) {
						taggedAsRetryable = true
					}
				}
				s.True(taggedAsRetryable, "logged error should be tagged as retryable")
			} else if strings.Contains(msg, "acquired shard") {
				s.FailNow("should not acquire shard")
			}
		case <-ctx.Done():
			s.FailNow("timed out waiting for retry")
		}
	}
}

// EventualSuccessSuite is the test suite for testing what happens when acquire shard returns a deadline exceeded error
// followed by a successful acquire shard call.
type EventualSuccessSuite struct {
	AcquireShardFunctionalSuite
}

// SetupSuite reads the targeted eventual success fault injection config from the testdata folder.
// This config deterministically causes the first acquire shard call to return a deadline exceeded error, and it causes
// the next call to return a successful response.
func (s *EventualSuccessSuite) SetupSuite() {
	s.AcquireShardFunctionalSuite.SetupSuite()
	s.setupSuite("testdata/acquire_shard_eventual_success.yaml")
}

// TestEventuallySucceeds verifies that we eventually succeed in acquiring the shard when we get a deadline exceeded
// error followed by a successful acquire shard call.
func (s *EventualSuccessSuite) TestEventuallySucceeds() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	numErrors := 0
	for {
		select {
		case record := <-s.logs:
			msg := strings.ToLower(record.msg)
			if strings.Contains(msg, "error acquiring shard") {
				numErrors++
				taggedAsRetryable := false
				for _, tg := range record.tags {
					if tg == tag.IsRetryable(true) {
						taggedAsRetryable = true
					}
				}
				s.True(taggedAsRetryable, "logged error should be tagged as retryable")
			} else if strings.Contains(msg, "acquired shard") {
				s.Equal(1, numErrors, "should succeed after one error")
				return
			} else if strings.Contains(msg, "couldn't acquire shard") {
				s.FailNow("should not fail to acquire shard")
			}
		case <-ctx.Done():
			s.FailNow("timed out waiting for retry")
		}
	}
}
