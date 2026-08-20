package tests

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/tests/testcore"
)

const historyStartFailureLog = "Nexus StartOperation request failed"

type nexusLogRecord struct {
	message string
	tags    []tag.Tag
}

type nexusHandlerRequest struct {
	service   string
	operation string
	requestID string
}

type nexusLogRecorder struct {
	log.Logger
	testLogger *testlogger.TestLogger
	mu         sync.Mutex
	records    []nexusLogRecord
}

func newNexusLogRecorder(t *testing.T) *nexusLogRecorder {
	t.Helper()
	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	testlogger.DontFailOnError(testLogger)
	return &nexusLogRecorder{
		Logger:     testLogger,
		testLogger: testLogger,
	}
}

func (r *nexusLogRecorder) Error(message string, tags ...tag.Tag) {
	r.record(message, tags)
	r.Logger.Error(message, tags...)
}

func (r *nexusLogRecorder) record(message string, tags []tag.Tag) {
	r.mu.Lock()
	r.records = append(r.records, nexusLogRecord{
		message: message,
		tags:    append([]tag.Tag(nil), tags...),
	})
	r.mu.Unlock()
}

func (r *nexusLogRecorder) expect(level testlogger.Level, message string, tags ...tag.Tag) *testlogger.Expectation {
	return r.testLogger.Expect(level, "^"+regexp.QuoteMeta(message)+"$", tags...)
}

func (r *nexusLogRecorder) matchingRecords(message string, requiredTags map[string]string) []nexusLogRecord {
	r.mu.Lock()
	defer r.mu.Unlock()

	var matches []nexusLogRecord
	for _, record := range r.records {
		if record.message != message {
			continue
		}
		values := make(map[string]string, len(record.tags))
		for _, field := range record.tags {
			values[field.Key()] = fmt.Sprint(field.Value())
		}
		matched := true
		for key, value := range requiredTags {
			if values[key] != value {
				matched = false
				break
			}
		}
		if matched {
			matches = append(matches, record)
		}
	}
	return matches
}

func nexusLogTagValue(t require.TestingT, record nexusLogRecord, key string) string {
	for i := len(record.tags) - 1; i >= 0; i-- {
		if record.tags[i].Key() == key {
			return fmt.Sprint(record.tags[i].Value())
		}
	}
	require.FailNow(t, "required Nexus log tag is missing", "tag: %s, record: %#v", key, record)
	return ""
}

type NexusLoggingSuite struct {
	parallelsuite.Suite[*NexusLoggingSuite]
}

func TestNexusLoggingSuiteHSM(t *testing.T) {
	parallelsuite.Run(t, &NexusLoggingSuite{}, false)
}

func TestNexusLoggingSuiteCHASM(t *testing.T) {
	parallelsuite.Run(t, &NexusLoggingSuite{}, true)
}

func (s *NexusLoggingSuite) newTestEnv(chasmEnabled bool, logger log.Logger) *NexusTestEnv {
	rolloutPercent := 0
	if chasmEnabled {
		rolloutPercent = 100
	}
	return newNexusTestEnv(s.T(), true,
		testcore.WithLogger(logger),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, chasmEnabled),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, chasmEnabled),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, chasmEnabled),
		testcore.WithDynamicConfig(chasmnexus.ChasmWorkflowOperationsRolloutPercent, rolloutPercent),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSignalBacklinks, chasmEnabled),
	)
}

func (s *NexusLoggingSuite) TestStartFailureCarriesCorrelationAcrossHop(chasmEnabled bool) {
	recorder := newNexusLogRecorder(s.T())
	env := s.newTestEnv(chasmEnabled, recorder)

	ctx := s.Context()
	callerTaskQueue := testcore.RandomizeStr(s.T().Name() + "-caller")
	serviceName := "test-service"
	operationName := testcore.RandomizeStr("logging-operation")
	handlerRequests := make(chan nexusHandlerRequest, 1)
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), nexustest.Handler{
		OnStartOperation: func(_ context.Context, service, operation string, _ *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case handlerRequests <- nexusHandlerRequest{service: service, operation: operation, requestID: options.RequestID}:
			default:
			}
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "intentional failure")
		},
	})
	expectation := recorder.expect(testlogger.Error, historyStartFailureLog,
		tag.WorkflowNamespace(env.Namespace().String()),
		tag.Endpoint(endpointName),
		tag.NexusOperation(operationName),
		tag.Operation("StartOperation"),
	)

	callerWorkflow := func(ctx workflow.Context) error {
		nexusClient := workflow.NewNexusClient(endpointName, serviceName)
		return nexusClient.ExecuteOperation(ctx, operationName, nil, workflow.NexusOperationOptions{
			ScheduleToCloseTimeout: 3 * time.Second,
		}).Get(ctx, nil)
	}

	callerWorker := worker.New(env.SdkClient(), callerTaskQueue, worker.Options{})
	callerWorker.RegisterWorkflow(callerWorkflow)
	s.NoError(callerWorker.Start())
	s.T().Cleanup(callerWorker.Stop)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: callerTaskQueue,
	}, callerWorkflow)
	s.NoError(err)

	commonTags := map[string]string{
		tag.WorkflowNamespace("").Key(): env.Namespace().String(),
		tag.Endpoint("").Key():          endpointName,
		tag.NexusOperation("").Key():    operationName,
	}
	historyTags := map[string]string{
		tag.WorkflowNamespace("").Key(): env.Namespace().String(),
		tag.WorkflowID("").Key():        run.GetID(),
		tag.WorkflowRunID("").Key():     run.GetRunID(),
		tag.Endpoint("").Key():          endpointName,
		tag.NexusOperation("").Key():    operationName,
		tag.Operation("").Key():         "StartOperation",
	}

	s.Await(func(s *NexusLoggingSuite) {
		s.Require().True(expectation.Matched())
		s.Require().NotEmpty(recorder.matchingRecords(historyStartFailureLog, historyTags))
	}, 10*time.Second, 100*time.Millisecond)

	historyRecord := recorder.matchingRecords(historyStartFailureLog, historyTags)[0]
	requestIDKey := tag.RequestID("").Key()
	historyRequestID := nexusLogTagValue(s.T(), historyRecord, requestIDKey)
	s.Require().NotEmpty(historyRequestID)
	select {
	case handlerRequest := <-handlerRequests:
		s.Require().Equal(nexusHandlerRequest{
			service:   serviceName,
			operation: operationName,
			requestID: historyRequestID,
		}, handlerRequest)
	case <-ctx.Done():
		s.FailNow("timed out waiting for the Nexus handler request")
	}

	for key, value := range commonTags {
		s.Require().Equal(value, nexusLogTagValue(s.T(), historyRecord, key))
	}
	s.Require().NotEmpty(nexusLogTagValue(s.T(), historyRecord, tag.AttemptStart(time.Time{}).Key()))
	attempt, err := strconv.Atoi(nexusLogTagValue(s.T(), historyRecord, tag.Attempt(0).Key()))
	s.Require().NoError(err)
	s.Require().GreaterOrEqual(attempt, 0)
	s.Require().NotEmpty(nexusLogTagValue(s.T(), historyRecord, tag.Error(errors.New("failure")).Key()))

	s.NoError(env.SdkClient().TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "logging contract verified"))
}
