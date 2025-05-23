package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tests/testcore"
)

// AcquireShardSuiteBase is the base test suite for testing acquire shard.
type AcquireShardSuiteBase struct {
	testcore.FunctionalTestBase
	logRecorder *logRecorder
	logs        chan logRecord
}

// SetupSuite sets up the test suite by setting the log recorder.
func (s *AcquireShardSuiteBase) SetupSuite() {
	// Server startup happens in Setup_Suite_, but we don't listen on the log channel until the
	// _test_ runs. So this needs to be big enough to buffer all of the server startup logs,
	// which are currently about 190 lines (October 2024).
	s.logs = make(chan logRecord, 2000)
	s.logRecorder = newLogRecorder(s.logs)
	s.Logger = s.logRecorder
}

// TearDownSuite tears down the test suite by shutting down the test cluster after a short delay.
func (s *AcquireShardSuiteBase) TearDownSuite() {
	// we need to wait for all components to start before we can safely tear down
	time.Sleep(time.Second * 5) //nolint:forbidigo
	s.FunctionalTestBase.TearDownCluster()
}

// newLogRecorder creates a new log recorder. It records all the logs to the given channel.
func newLogRecorder(logs chan logRecord) *logRecorder {
	return &logRecorder{
		Logger: log.NewTestLogger(),
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
	r.Logger.Info(msg, tags...)
	r.record(msg, tags...)
}

// Error records the error message
func (r *logRecorder) Error(msg string, tags ...tag.Tag) {
	r.Logger.Error(msg, tags...)
	r.record(msg, tags...)
}

// Fatal ignores the fatal call
func (r *logRecorder) Fatal(msg string, tags ...tag.Tag) {
	r.Logger.Error("FATAL: "+msg, tags...)
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
		r.Logger.Warn("failed to deliver log message to listener: " + msg)
	}
}

// OwnershipLostErrorSuite is the test suite for testing what happens when acquire shard returns an ownership lost
// error.
type OwnershipLostErrorSuite struct {
	AcquireShardSuiteBase
}

// TestAcquireShard_OwnershipLostErrorSuite tests what happens when acquire shard returns an ownership lost error.
func TestAcquireShard_OwnershipLostErrorSuite(t *testing.T) {
	t.Parallel()
	s := new(OwnershipLostErrorSuite)
	suite.Run(t, s)
}

// SetupSuite reads the shard ownership lost error fault injection config from the testdata folder.
func (s *OwnershipLostErrorSuite) SetupSuite() {
	s.AcquireShardSuiteBase.SetupSuite()
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithNumHistoryShards(1),
		testcore.WithFaultInjectionConfig((&config.FaultInjection{}).
			WithError(config.ShardStoreName, "UpdateShard", "ShardOwnershipLost", 1.0),
		))
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
	AcquireShardSuiteBase
}

// TestAcquireShard_DeadlineExceededErrorSuite tests what happens when acquire shard returns a deadline exceeded error
func TestAcquireShard_DeadlineExceededErrorSuite(t *testing.T) {
	t.Parallel()
	s := new(DeadlineExceededErrorSuite)
	suite.Run(t, s)
}

// SetupSuite reads the deadline exceeded error targeted fault injection config from the test data folder.
func (s *DeadlineExceededErrorSuite) SetupSuite() {
	s.AcquireShardSuiteBase.SetupSuite()
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithNumHistoryShards(1),
		testcore.WithFaultInjectionConfig((&config.FaultInjection{}).
			WithError(config.ShardStoreName, "UpdateShard", "DeadlineExceeded", 1.0),
		))
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
	AcquireShardSuiteBase
}

// TestAcquireShard_EventualSuccess verifies that we eventually succeed in acquiring the shard when we get a deadline
// exceeded error followed by a successful acquire shard call.
// To make this test deterministic, we set the seed to in the config file to a fixed value.
func TestAcquireShard_EventualSuccess(t *testing.T) {
	t.Parallel()
	s := new(EventualSuccessSuite)
	suite.Run(t, s)
}

// SetupSuite reads the targeted eventual success fault injection config from the testdata folder.
// This config deterministically causes the first acquire shard call to return a deadline exceeded error, and it causes
// the next call to return a successful response.
func (s *EventualSuccessSuite) SetupSuite() {
	s.AcquireShardSuiteBase.SetupSuite()
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithNumHistoryShards(1),
		testcore.WithFaultInjectionConfig((&config.FaultInjection{}).
			WithError(config.ShardStoreName, "UpdateShard", "DeadlineExceeded", 0.5).
			WithMethodSeed(config.ShardStoreName, "UpdateShard", 43),
		))
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
