package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	errordetailspb "go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *NamespaceInterceptorTestSuite) TestRetryDelayFloor() {
	s.Run("concurrent limit", func(s *NamespaceInterceptorTestSuite) {
		for _, tc := range []struct {
			name          string
			floor         time.Duration
			wantDelay     time.Duration
			checkWireData bool
		}{
			{name: "floor", floor: 2 * time.Second, wantDelay: 2 * time.Second},
			{name: "unset", wantDelay: 0},
			{name: "large floor", floor: time.Hour, wantDelay: time.Hour},
			{name: "serialized legacy detail", floor: 2 * time.Second, wantDelay: 2 * time.Second, checkWireData: true},
		} {
			s.Run(tc.name, func(s *NamespaceInterceptorTestSuite) {
				t := s.T()
				opts := []testcore.TestOption{
					testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentLongRunningRequestsPerInstance, 1),
					testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
					testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
				}
				if tc.floor > 0 {
					opts = append(opts, testcore.WithDynamicConfig(dynamicconfig.FrontendResourceExhaustedRetryDelayFloor, tc.floor))
				}
				env := testcore.NewEnv(t, opts...)
				assertConcurrentPollRejected(s.Context(), t, env, tc.wantDelay, tc.checkWireData)
			})
		}
	})

	s.Run("namespace override precedence", func(s *NamespaceInterceptorTestSuite) {
		t := s.T()
		env := testcore.NewEnv(t,
			testcore.WithDedicatedCluster(),
			testcore.WithDynamicConfig(dynamicconfig.FrontendRPS, 2400),
			testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentLongRunningRequestsPerInstance, 1),
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
			testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
		)
		env.OverrideDynamicConfig(dynamicconfig.FrontendResourceExhaustedRetryDelayFloor, []dynamicconfig.ConstrainedValue{{
			Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
			Value:       4 * time.Second,
		}, {
			Value: time.Second,
		}})
		assertConcurrentPollRejected(s.Context(), t, env, 4*time.Second, false)
	})

	s.Run("namespace rate limit", func(s *NamespaceInterceptorTestSuite) {
		t := s.T()
		env := testcore.NewEnv(t)
		rateLimitNamespace := "retry-delay-floor-rps-" + uuid.NewString()
		// Use a second namespace so its limiter is created after these settings are installed.
		overrideForRateLimitNamespace := func(setting dynamicconfig.GenericSetting, value any) {
			env.GetTestCluster().OverrideDynamicConfig(t, setting, []dynamicconfig.ConstrainedValue{{
				Constraints: dynamicconfig.Constraints{Namespace: rateLimitNamespace},
				Value:       value,
			}})
		}
		overrideForRateLimitNamespace(dynamicconfig.FrontendMaxNamespaceRPSPerInstance, 1)
		overrideForRateLimitNamespace(dynamicconfig.FrontendMaxNamespaceBurstRatioPerInstance, float64(1))
		overrideForRateLimitNamespace(dynamicconfig.FrontendResourceExhaustedRetryDelayFloor, 2*time.Second)

		ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer cancel()
		_, err := env.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
			Namespace:                        rateLimitNamespace,
			Description:                      "namespace for retry delay floor rate limit test",
			WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
			HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
		})
		require.NoError(t, err)

		var rejected error
		successfulRequests := 0
		for range 10 {
			_, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: rateLimitNamespace,
				Execution: &commonpb.WorkflowExecution{WorkflowId: uuid.NewString()},
			})
			if _, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok {
				rejected = err
				break
			}
			_, notFound := errors.AsType[*serviceerror.NotFound](err)
			require.True(t, notFound, "expected missing execution before rate limit, got: %v", err)
			successfulRequests++
		}
		require.Error(t, rejected, "rapid RPCs did not reach the namespace rate limit")
		require.Positive(t, successfulRequests, "the namespace rate limit rejected every RPC")
		assertResourceExhaustedRetryDelay(t, rejected, enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, 2*time.Second)
	})
}

func (s *NamespaceInterceptorTestSuite) TestRetryDelayFloorHTTPAPI() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.FrontendResourceExhaustedRetryDelayFloor, 2*time.Second),
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxNamespaceRPSPerInstance, 1),
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxNamespaceBurstRatioPerInstance, float64(1)),
	)
	rateLimitNamespace := "retry-delay-floor-http-" + uuid.NewString()
	overrideForRateLimitNamespace := func(setting dynamicconfig.GenericSetting, value any) {
		env.GetTestCluster().OverrideDynamicConfig(s.T(), setting, []dynamicconfig.ConstrainedValue{{
			Constraints: dynamicconfig.Constraints{Namespace: rateLimitNamespace},
			Value:       value,
		}})
	}
	overrideForRateLimitNamespace(dynamicconfig.FrontendMaxNamespaceRPSPerInstance, 1)
	overrideForRateLimitNamespace(dynamicconfig.FrontendMaxNamespaceBurstRatioPerInstance, float64(1))
	overrideForRateLimitNamespace(dynamicconfig.FrontendResourceExhaustedRetryDelayFloor, 2*time.Second)

	ctx, cancel := context.WithTimeout(s.Context(), 5*time.Second)
	defer cancel()
	_, err := env.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        rateLimitNamespace,
		Description:                      "namespace for HTTP retry delay floor test",
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	require.NoError(s.T(), err)

	requestBody, err := protojson.Marshal(&workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		WorkflowType: &commonpb.WorkflowType{Name: "retry-delay-floor-http"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue()},
	})
	require.NoError(s.T(), err)
	workflowID := uuid.NewString()
	url := "http://" + env.HttpAPIAddress() + "/namespaces/" + rateLimitNamespace + "/workflows/" + workflowID
	type response struct {
		statusCode int
		body       []byte
		err        error
	}
	const numRequests = 4
	start := make(chan struct{})
	responses := make(chan response, numRequests)
	var requests sync.WaitGroup
	requests.Add(numRequests)
	for range numRequests {
		go func() {
			defer requests.Done()
			<-start
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(requestBody))
			if err != nil {
				responses <- response{err: err}
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				responses <- response{err: err}
				return
			}
			body, err := io.ReadAll(resp.Body)
			closeErr := resp.Body.Close()
			if err == nil {
				err = closeErr
			}
			responses <- response{statusCode: resp.StatusCode, body: body, err: err}
		}()
	}
	close(start)
	requests.Wait()
	var retryStatusBody []byte
	sawNonRejectedResponse := false
	for range numRequests {
		result := <-responses
		require.NoError(s.T(), result.err)
		if result.statusCode == http.StatusTooManyRequests {
			retryStatusBody = result.body
		} else {
			sawNonRejectedResponse = true
		}
	}
	require.True(s.T(), sawNonRejectedResponse, "all concurrent HTTP requests were rate limited")
	require.NotEmpty(s.T(), retryStatusBody, "concurrent HTTP requests did not reach the namespace rate limit")

	var responseStatus struct {
		Code    int               `json:"code"`
		Details []json.RawMessage `json:"details"`
	}
	require.NoError(s.T(), json.Unmarshal(retryStatusBody, &responseStatus))
	require.Equal(s.T(), int(codes.ResourceExhausted), responseStatus.Code)
	require.Len(s.T(), responseStatus.Details, 2)
	var resourceExhaustedDetail struct {
		Type  string `json:"@type"`
		Cause string `json:"cause"`
		Scope string `json:"scope"`
	}
	require.NoError(s.T(), json.Unmarshal(responseStatus.Details[0], &resourceExhaustedDetail))
	require.Equal(s.T(), "type.googleapis.com/temporal.api.errordetails.v1.ResourceExhaustedFailure", resourceExhaustedDetail.Type)
	require.Equal(s.T(), "RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT", resourceExhaustedDetail.Cause)
	require.Equal(s.T(), "RESOURCE_EXHAUSTED_SCOPE_NAMESPACE", resourceExhaustedDetail.Scope)
	var retryInfoDetail struct {
		Type       string `json:"@type"`
		RetryDelay string `json:"retryDelay"`
	}
	require.NoError(s.T(), json.Unmarshal(responseStatus.Details[1], &retryInfoDetail))
	require.Equal(s.T(), "type.googleapis.com/google.rpc.RetryInfo", retryInfoDetail.Type)
	require.Equal(s.T(), "2s", retryInfoDetail.RetryDelay)
}

func assertResourceExhaustedRetryDelay(t *testing.T, err error, cause enumspb.ResourceExhaustedCause, wantDelay time.Duration) {
	t.Helper()
	var resourceExhausted *serviceerror.ResourceExhausted
	require.ErrorAs(t, err, &resourceExhausted)
	require.Equal(t, cause, resourceExhausted.Cause)
	require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, resourceExhausted.Scope)

	// The RPC client converts received statuses into Temporal errors, so use its status helper to retain wire details.
	receivedStatus := serviceerror.ToStatus(err)
	wantDetailCount := 1
	if wantDelay > 0 {
		wantDetailCount++
	}
	require.Len(t, receivedStatus.Proto().GetDetails(), wantDetailCount)
	details := receivedStatus.Details()
	require.Len(t, details, wantDetailCount)
	require.IsType(t, &errordetailspb.ResourceExhaustedFailure{}, details[0])
	protorequire.ProtoEqual(t, &errordetailspb.ResourceExhaustedFailure{Cause: cause, Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE}, details[0].(*errordetailspb.ResourceExhaustedFailure))

	gotDelay, hasDelay := retryInfoDelay(err)
	if wantDelay == 0 {
		require.False(t, hasDelay)
		return
	}
	require.True(t, hasDelay)
	require.Equal(t, wantDelay, gotDelay)
	require.IsType(t, &errdetails.RetryInfo{}, details[1])
	protorequire.ProtoEqual(t, &errdetails.RetryInfo{RetryDelay: durationpb.New(wantDelay)}, details[1].(*errdetails.RetryInfo))
}

func retryInfoDelay(err error) (time.Duration, bool) {
	for _, detail := range serviceerror.ToStatus(err).Details() {
		if retryInfo, ok := detail.(*errdetails.RetryInfo); ok && retryInfo.GetRetryDelay() != nil {
			return retryInfo.GetRetryDelay().AsDuration(), true
		}
	}
	return 0, false
}

func assertConcurrentPollRejected(ctx context.Context, t *testing.T, env *testcore.TestEnv, wantDelay time.Duration, checkWireData bool) {
	t.Helper()
	queue := &taskqueuepb.TaskQueue{Name: uuid.NewString()}
	identity := uuid.NewString()
	pollCtx, cancel := context.WithCancel(ctx)
	pollDone := make(chan error, 1)
	go func() {
		_, err := env.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: queue,
			Identity:  identity,
		})
		pollDone <- err
	}()
	defer func() {
		cancel()
		requireCanceledPoll(t, await.Rcv(t, pollDone))
	}()

	await.Require(ctx, t, func(t *await.T) {
		resp, err := env.GetTestCluster().MatchingClient().DescribeTaskQueuePartition(t.Context(),
			&matchingservice.DescribeTaskQueuePartitionRequest{
				NamespaceId: env.NamespaceID().String(),
				TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
					TaskQueue:     queue.GetName(),
					TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				},
				Versions:      &taskqueuepb.TaskQueueVersionSelection{Unversioned: true},
				ReportPollers: true,
			})
		require.NoError(t, err)
		var identities []string
		for _, poller := range resp.GetVersionsInfoInternal()[""].GetPhysicalTaskQueueInfo().GetPollers() {
			identities = append(identities, poller.GetIdentity())
		}
		require.Contains(t, identities, identity)
	}, 5*time.Second, 50*time.Millisecond)

	callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
	defer callCancel()
	_, err := env.FrontendClient().PollWorkflowTaskQueue(callCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: queue,
		Identity:  "limited-" + uuid.NewString(),
	})
	assertResourceExhaustedRetryDelay(t, err, enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, wantDelay)
	if checkWireData {
		assertSerializedConcurrentLimitDetail(t, err)
	}
}

func assertSerializedConcurrentLimitDetail(t *testing.T, err error) {
	t.Helper()
	wireStatus := serviceerror.ToStatus(err).Proto()
	statusBytes, marshalErr := proto.Marshal(wireStatus)
	require.NoError(t, marshalErr)
	decoded := &statuspb.Status{}
	require.NoError(t, proto.Unmarshal(statusBytes, decoded))
	details := status.FromProto(decoded).Details()
	require.NotEmpty(t, details)
	require.IsType(t, &errordetailspb.ResourceExhaustedFailure{}, details[0])
	protorequire.ProtoEqual(t,
		&errordetailspb.ResourceExhaustedFailure{
			Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		},
		details[0].(*errordetailspb.ResourceExhaustedFailure),
	)
}

func requireCanceledPoll(t *testing.T, err error) {
	t.Helper()
	require.True(t, common.IsContextCanceledErr(err), "poll returned before cancellation: %v", err)
}
