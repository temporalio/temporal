package tests

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/faultinjection"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

// AcquireShardSuite tests the shard-acquisition retry behavior under injected
// persistence faults. Each test method spins up its own dedicated single-shard
// cluster wired to a custom logRecorder, so it can assert on shard-acquisition
// log messages.
type AcquireShardSuite struct {
	parallelsuite.Suite[*AcquireShardSuite]
}

func TestAcquireShardSuite(t *testing.T) {
	parallelsuite.Run(t, &AcquireShardSuite{})
}

// logRecord represents a call to Info or Error.
type logRecord struct {
	msg  string
	tags []tag.Tag
}

// logRecorder records log messages to a channel so tests can assert on them.
type logRecorder struct {
	log.Logger
	logs chan logRecord
}

// newLogRecorder creates a new log recorder. It records all the logs to the given channel.
func newLogRecorder(logs chan logRecord) *logRecorder {
	return &logRecorder{
		Logger: log.NewTestLogger(),
		logs:   logs,
	}
}

// Info records the info message.
func (r *logRecorder) Info(msg string, tags ...tag.Tag) {
	r.Logger.Info(msg, tags...)
	r.record(msg, tags...)
}

// Error records the error message.
func (r *logRecorder) Error(msg string, tags ...tag.Tag) {
	r.Logger.Error(msg, tags...)
	r.record(msg, tags...)
}

// Fatal downgrades fatal calls so that cluster-teardown fatals from other
// components do not abort the test process.
func (r *logRecorder) Fatal(msg string, tags ...tag.Tag) {
	r.Logger.Error("FATAL: "+msg, tags...)
}

// record sends a logRecord to the logs channel. If there is no active receiver, the logRecord will be dropped.
func (r *logRecorder) record(msg string, tags ...tag.Tag) {
	select {
	case r.logs <- logRecord{
		msg:  msg,
		tags: tags,
	}:
	default:
		r.Warn("failed to deliver log message to listener: " + msg)
	}
}

// newEnv builds a logs channel + logRecorder and starts a dedicated single-shard
// cluster wired to that recorder with the given fault injection option. The
// cluster is torn down automatically via t.Cleanup inside NewEnv.
func (s *AcquireShardSuite) newEnv(faultOpt testcore.TestOption) chan logRecord {
	// Server startup happens when the cluster is created, but the test doesn't
	// listen on the log channel until afterward. So the buffer needs to be big
	// enough to hold all server-startup logs, which are currently about 190 lines.
	logs := make(chan logRecord, 2000)
	testcore.NewEnv(
		s.T(),
		testcore.WithLogger(newLogRecorder(logs)),
		testcore.WithHistoryShardCount(1),
		faultOpt,
	)
	return logs
}

// TestOwnershipLost_DoesNotRetry verifies that we do not retry acquiring the shard
// when we get an ownership lost error.
func (s *AcquireShardSuite) TestOwnershipLost_DoesNotRetry() {
	reg := faultinjection.NewFaultRegistry()
	logs := s.newEnv(testcore.InjectPersistenceFault(s.T(), reg,
		func(faultinjection.Target) error {
			return &persistence.ShardOwnershipLostError{Msg: "injected shard ownership lost"}
		},
		testcore.WithStore(config.ShardStoreName),
		testcore.WithMethod("UpdateShard")))

	ctx, cancel := context.WithTimeout(s.Context(), time.Second*10)
	defer cancel()

	numAttempts := 0
	for {
		select {
		case record := <-logs:
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

// TestDeadlineExceeded_DoesRetry verifies that we do retry acquiring the shard when
// we get a deadline exceeded error because that should be considered a transient error.
func (s *AcquireShardSuite) TestDeadlineExceeded_DoesRetry() {
	reg := faultinjection.NewFaultRegistry()
	logs := s.newEnv(testcore.InjectPersistenceFault(s.T(), reg,
		func(faultinjection.Target) error {
			return context.DeadlineExceeded
		},
		testcore.WithStore(config.ShardStoreName),
		testcore.WithMethod("UpdateShard")))

	ctx, cancel := context.WithTimeout(s.Context(), time.Second*10)
	defer cancel()

	for numAttempts := 0; numAttempts < 2; {
		select {
		case record := <-logs:
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

// TestEventualSuccess verifies that we eventually succeed in acquiring the shard when
// we get a deadline exceeded error followed by a successful acquire shard call.
// The fault fails exactly the first UpdateShard call and then lets subsequent calls
// through, so the test is deterministic without relying on a fixed RNG seed.
func (s *AcquireShardSuite) TestEventualSuccess() {
	reg := faultinjection.NewFaultRegistry()
	var updateShardCalls atomic.Int32
	logs := s.newEnv(testcore.InjectPersistenceFault(s.T(), reg,
		func(faultinjection.Target) error {
			if updateShardCalls.Add(1) == 1 {
				return context.DeadlineExceeded
			}
			return nil
		},
		testcore.WithStore(config.ShardStoreName),
		testcore.WithMethod("UpdateShard")))

	ctx, cancel := context.WithTimeout(s.Context(), time.Second*10)
	defer cancel()

	numErrors := 0
	for {
		select {
		case record := <-logs:
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
