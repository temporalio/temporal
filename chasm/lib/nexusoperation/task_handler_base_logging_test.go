package nexusoperation

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestNexusTaskHandlerBaseLogCallFailure(t *testing.T) {
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	attemptStart := time.Date(2026, time.August, 20, 10, 0, 0, 0, time.UTC)
	callErr := errors.New("call failed")
	expectation := logger.Expect(
		testlogger.Error,
		"^Nexus StartOperation request failed$",
		tag.Operation("StartOperation"),
		tag.WorkflowNamespace("caller-namespace"),
		tag.NexusEndpointTargetNamespaceID("target-namespace-id"),
		tag.RequestID("request-id"),
		tag.NexusOperation("nexus-operation"),
		tag.Endpoint("endpoint"),
		tag.WorkflowID("workflow-id"),
		tag.WorkflowRunID("run-id"),
		tag.Attempt(2),
		tag.Error(callErr),
	)

	handler := nexusTaskHandlerBase{logger: logger}
	handler.logCallFailure(invocationTraceContext{
		operationTag:      "StartOperation",
		namespaceName:     "caller-namespace",
		targetNamespaceID: "target-namespace-id",
		requestID:         "request-id",
		operation:         "nexus-operation",
		endpointName:      "endpoint",
		workflowID:        "workflow-id",
		runID:             "run-id",
		attemptStart:      attemptStart,
		attempt:           2,
	}, callErr, "")

	require.True(t, expectation.Matched())
}
