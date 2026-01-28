package tests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"
)

type SomeJSONStruct struct {
	SomeField string `json:"someField"`
}

func jsonPayload(data string) *commonpb.Payloads {
	return &commonpb.Payloads{
		Payloads: []*commonpb.Payload{{
			Metadata: map[string][]byte{
				converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON),
			},
			Data: []byte(data),
		}},
	}
}

// httpAPITestHelper contains helper methods for HTTP API tests.
type httpAPITestHelper struct {
	t              *testing.T
	httpAPIAddress string
	namespace      string
}

func (h *httpAPITestHelper) httpGet(expectedStatus int, url, contentType string) (*http.Response, []byte) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		h.t.Fatalf("Failed to create GET request: %v", err)
	}
	if contentType != "" {
		req.Header.Add("Accept", contentType)
		req.Header.Add("Content-Type", contentType)
	}
	h.t.Logf("GET %s (Accept: %s)", url, contentType)
	return h.httpRequest(expectedStatus, req)
}

func (h *httpAPITestHelper) httpPost(expectedStatus int, url, contentType, jsonBody string) (*http.Response, []byte) {
	req, err := http.NewRequest("POST", url, strings.NewReader(jsonBody))
	if err != nil {
		h.t.Fatalf("Failed to create POST request: %v", err)
	}
	req.Header.Add("Accept", contentType)
	req.Header.Add("Content-Type", contentType)
	h.t.Logf("POST %s (Accept: %s)", url, contentType)
	return h.httpRequest(expectedStatus, req)
}

func (h *httpAPITestHelper) httpRequest(expectedStatus int, req *http.Request) (*http.Response, []byte) {
	if req.URL.Scheme == "" {
		req.URL.Scheme = "http"
	}
	if req.URL.Host == "" {
		req.URL.Host = h.httpAPIAddress
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("HTTP request failed: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		h.t.Fatalf("Failed to read response body: %v", err)
	}
	if resp.StatusCode != expectedStatus {
		h.t.Fatalf("Bad status %d (expected %d), body: %s", resp.StatusCode, expectedStatus, body)
	}
	return resp, body
}

func runHTTPAPIBasicsTest(
	t *testing.T,
	h *httpAPITestHelper,
	worker sdkworker.Worker,
	taskQueue string,
	capture *metrics.CapturedRecordings,
	contentType string,
	startWFRequestBody, queryBody, signalBody func() string,
	verifyQueryResult, verifyHistory func(*testing.T, []byte),
) {
	// Create basic workflow that can answer queries, get signals, etc
	workflowFn := func(ctx workflow.Context, arg *SomeJSONStruct) (*SomeJSONStruct, error) {
		// Query that just returns query arg
		err := workflow.SetQueryHandler(ctx, "some-query", func(queryArg *SomeJSONStruct) (*SomeJSONStruct, error) {
			return queryArg, nil
		})
		if err != nil {
			return nil, err
		}
		// Wait for signal to complete
		var done bool
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(workflow.GetSignalChannel(ctx, "some-signal"), func(ch workflow.ReceiveChannel, _ bool) {
			var signalArg SomeJSONStruct
			ch.Receive(ctx, &signalArg)
			if signalArg.SomeField != "signal-arg" {
				panic("invalid signal arg")
			}
			done = true
		})
		for !done {
			sel.Select(ctx)
		}
		return arg, nil
	}
	worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: "http-basic-workflow"})

	// Start
	workflowID := testcore.RandomizeStr("wf")
	_, respBody := h.httpPost(http.StatusOK, "/namespaces/"+h.namespace+"/workflows/"+workflowID, contentType, startWFRequestBody())
	var startResp struct {
		RunID string `json:"runId"`
	}
	if err := json.Unmarshal(respBody, &startResp); err != nil {
		t.Fatalf("Failed to unmarshal start response: %v", err)
	}

	// Check that there is a an HTTP call metric with the proper tags/value. We
	// can't test overall counts because the metrics handler is shared across
	// concurrently executing tests.
	var found bool
	for _, metric := range capture.Snapshot()[metrics.HTTPServiceRequests.Name()] {
		found =
			metric.Tags[metrics.OperationTagName] == "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution" &&
				metric.Tags["namespace"] == h.namespace &&
				metric.Value == int64(1)
		if found {
			break
		}
	}
	if !found {
		t.Fatal("Expected HTTP service request metric not found")
	}

	// Confirm already exists error with details and proper code
	_, respBody = h.httpPost(http.StatusConflict, "/namespaces/"+h.namespace+"/workflows/"+workflowID, contentType, startWFRequestBody())
	var errResp struct {
		Message string `json:"message"`
		Details []struct {
			RunID string `json:"runId"`
		} `json:"details"`
	}
	if err := json.Unmarshal(respBody, &errResp); err != nil {
		t.Fatalf("Failed to unmarshal error response: %v", err)
	}
	if !strings.Contains(errResp.Message, "already running") {
		t.Fatalf("Expected 'already running' error, got: %s", errResp.Message)
	}
	if errResp.Details[0].RunID != startResp.RunID {
		t.Fatalf("RunID mismatch: %s != %s", errResp.Details[0].RunID, startResp.RunID)
	}

	// Query
	_, respBody = h.httpPost(
		http.StatusOK,
		"/namespaces/"+h.namespace+"/workflows/"+workflowID+"/query/some-query",
		contentType,
		queryBody(),
	)
	verifyQueryResult(t, respBody)

	// Signal which also completes the workflow
	h.httpPost(
		http.StatusOK,
		"/namespaces/"+h.namespace+"/workflows/"+workflowID+"/signal/some-signal",
		contentType,
		signalBody(),
	)

	// Confirm workflow complete
	_, respBody = h.httpGet(
		http.StatusOK,
		// Our version of gRPC gateway only supports integer enums in queries :-(
		"/namespaces/"+h.namespace+"/workflows/"+workflowID+"/history?historyEventFilterType=2",
		contentType,
	)
	verifyHistory(t, respBody)
}

func TestHTTPAPI(t *testing.T) {
	t.Run("Basics_Protojson", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())

		if s.HttpAPIAddress() == "" {
			t.Skip("HTTP API server not enabled")
		}

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		s.NoError(err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
		s.NoError(worker.Start())
		defer worker.Stop()

		// Capture metrics
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		h := &httpAPITestHelper{
			t:              t,
			httpAPIAddress: s.HttpAPIAddress(),
			namespace:      s.Namespace().String(),
		}

		runHTTPAPIBasicsTest_Protojson(t, h, worker, taskQueue, capture, "application/json+no-payload-shorthand", false)
	})

	t.Run("Basics_ProtojsonPretty", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())

		if s.HttpAPIAddress() == "" {
			t.Skip("HTTP API server not enabled")
		}

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		s.NoError(err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
		s.NoError(worker.Start())
		defer worker.Stop()

		// Capture metrics
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		h := &httpAPITestHelper{
			t:              t,
			httpAPIAddress: s.HttpAPIAddress(),
			namespace:      s.Namespace().String(),
		}

		runHTTPAPIBasicsTest_Protojson(t, h, worker, taskQueue, capture, "application/json+pretty+no-payload-shorthand", true)
	})

	t.Run("Basics_Shorthand", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())

		if s.HttpAPIAddress() == "" {
			t.Skip("HTTP API server not enabled")
		}

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		s.NoError(err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
		s.NoError(worker.Start())
		defer worker.Stop()

		// Capture metrics
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		h := &httpAPITestHelper{
			t:              t,
			httpAPIAddress: s.HttpAPIAddress(),
			namespace:      s.Namespace().String(),
		}

		runHTTPAPIBasicsTest_Shorthand(t, h, worker, taskQueue, capture, "application/json", false)
	})

	t.Run("Basics_ShorthandPretty", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())

		if s.HttpAPIAddress() == "" {
			t.Skip("HTTP API server not enabled")
		}

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		s.NoError(err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})
		s.NoError(worker.Start())
		defer worker.Stop()

		// Capture metrics
		capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()
		defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

		h := &httpAPITestHelper{
			t:              t,
			httpAPIAddress: s.HttpAPIAddress(),
			namespace:      s.Namespace().String(),
		}

		runHTTPAPIBasicsTest_Shorthand(t, h, worker, taskQueue, capture, "application/json+pretty", true)
	})

	t.Run("HostValidation", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())

		if s.HttpAPIAddress() == "" {
			t.Skip("HTTP API server not enabled")
		}

		s.OverrideDynamicConfig(dynamicconfig.FrontendHTTPAllowedHosts, []string{"allowed"})

		h := &httpAPITestHelper{
			t:              t,
			httpAPIAddress: s.HttpAPIAddress(),
			namespace:      s.Namespace().String(),
		}

		{
			req, err := http.NewRequest("GET", "/system-info", nil)
			s.NoError(err)
			req.Host = "allowed"
			req.Header.Add("Accept", "application/json")
			req.Header.Add("Content-Type", "application/json")
			h.httpRequest(http.StatusOK, req)
		}
		{
			req, err := http.NewRequest("GET", "/system-info", nil)
			s.NoError(err)
			req.Host = "not-allowed"
			req.Header.Add("Accept", "application/json")
			req.Header.Add("Content-Type", "application/json")
			h.httpRequest(http.StatusForbidden, req)
		}
	})

	t.Run("Headers", func(t *testing.T) {
		s := testcore.NewEnv(t, testcore.WithDedicatedCluster())

		if s.HttpAPIAddress() == "" {
			t.Skip("HTTP API server not enabled")
		}

		h := &httpAPITestHelper{
			t:              t,
			httpAPIAddress: s.HttpAPIAddress(),
			namespace:      s.Namespace().String(),
		}

		// Make a claim mapper and authorizer that capture info
		var lastInfo *authorization.AuthInfo
		var listWorkflowMetadata metadata.MD
		var callbackLock sync.RWMutex
		s.GetTestCluster().Host().SetOnGetClaims(func(info *authorization.AuthInfo) (*authorization.Claims, error) {
			callbackLock.Lock()
			defer callbackLock.Unlock()
			if info != nil {
				lastInfo = info
			}
			return &authorization.Claims{System: authorization.RoleAdmin}, nil
		})
		s.GetTestCluster().Host().SetOnAuthorize(func(
			ctx context.Context,
			caller *authorization.Claims,
			target *authorization.CallTarget,
		) (authorization.Result, error) {
			callbackLock.Lock()
			defer callbackLock.Unlock()
			if target.APIName == "/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions" {
				listWorkflowMetadata, _ = metadata.FromIncomingContext(ctx)
			}
			return authorization.Result{Decision: authorization.DecisionAllow}, nil
		})

		// Make a simple list call that we don't care about the result
		req, err := http.NewRequest("GET", "/namespaces/"+s.Namespace().String()+"/workflows", nil)
		s.NoError(err)
		req.Header.Set("Authorization", "my-auth-token")
		req.Header.Set("X-Forwarded-For", "1.2.3.4:5678")
		// These headers are set to forward deep in the onebox config
		req.Header.Set("This-Header-Forwarded", "some-value")
		req.Header.Set("This-Header-Prefix-Forwarded-Foo", "foo")
		req.Header.Set("This-Header-Prefix-Forwarded-Bar", "bar")
		req.Header.Set("This-Header-Not-Forwarded", "some-value")
		h.httpRequest(http.StatusOK, req)

		// Confirm the claims got my auth token
		callbackLock.RLock()
		defer callbackLock.RUnlock()
		s.Equal("my-auth-token", lastInfo.AuthToken)

		// Check headers
		s.Equal("my-auth-token", listWorkflowMetadata["authorization"][0])
		s.Contains(listWorkflowMetadata["x-forwarded-for"][0], "1.2.3.4:5678")
		s.Equal("some-value", listWorkflowMetadata["this-header-forwarded"][0])
		s.Equal("foo", listWorkflowMetadata["this-header-prefix-forwarded-foo"][0])
		s.Equal("bar", listWorkflowMetadata["this-header-prefix-forwarded-bar"][0])
		s.NotContains(listWorkflowMetadata, "this-header-not-forwarded")
		s.Equal(headers.ClientNameServerHTTP, listWorkflowMetadata[headers.ClientNameHeaderName][0])
		s.Equal(headers.ServerVersion, listWorkflowMetadata[headers.ClientVersionHeaderName][0])
	})

	t.Run("Pretty", func(t *testing.T) {
		s := testcore.NewEnv(t)

		if s.HttpAPIAddress() == "" {
			t.Skip("HTTP API server not enabled")
		}

		h := &httpAPITestHelper{
			t:              t,
			httpAPIAddress: s.HttpAPIAddress(),
			namespace:      s.Namespace().String(),
		}

		// Make a call to system info normal, confirm no newline, then ask for pretty
		// and confirm newlines
		_, b := h.httpGet(http.StatusOK, "/system-info", "application/json")
		s.NotContains(b, byte('\n'))
		_, b = h.httpGet(http.StatusOK, "/system-info?pretty", "application/json")
		s.Contains(b, byte('\n'))
	})

	t.Run("OperatorService_ListSearchAttributes", func(t *testing.T) {
		s := testcore.NewEnv(t)

		if s.HttpAPIAddress() == "" {
			t.Skip("HTTP API server not enabled")
		}

		h := &httpAPITestHelper{
			t:              t,
			httpAPIAddress: s.HttpAPIAddress(),
			namespace:      s.Namespace().String(),
		}

		_, respBody := h.httpGet(
			http.StatusOK,
			"/cluster/namespaces/"+s.Namespace().String()+"/search-attributes",
			"application/json",
		)
		t.Log(string(respBody))
		var searchAttrsResp struct {
			CustomAttributes map[string]string `json:"customAttributes"`
			SystemAttributes map[string]string `json:"systemAttributes"`
			StorageSchema    map[string]string `json:"storageSchema"`
		}
		s.NoError(json.Unmarshal(respBody, &searchAttrsResp))
		// We don't allow for creating search attributes from the HTTP API yet, so
		// we just check that a few defaults exist. We don't want to check for all
		// of them as that's brittle and will break the tests if we ever add a new type
		s.Contains(searchAttrsResp.CustomAttributes, "CustomIntField")
		s.Equal(searchAttrsResp.CustomAttributes["CustomIntField"], "INDEXED_VALUE_TYPE_INT")
	})

	t.Run("Serves_OpenAPIv2_Docs", func(t *testing.T) {
		s := testcore.NewEnv(t)

		if s.HttpAPIAddress() == "" {
			t.Skip("HTTP API server not enabled")
		}

		h := &httpAPITestHelper{
			t:              t,
			httpAPIAddress: s.HttpAPIAddress(),
			namespace:      s.Namespace().String(),
		}

		_, respBody := h.httpGet(
			http.StatusOK,
			"/swagger.json",
			"",
		)
		var spec map[string]interface{}
		// We're not going to validate it here, just verify that it's valid
		s.NoError(json.Unmarshal(respBody, &spec), string(respBody))
	})

	t.Run("Serves_OpenAPIv3_Docs", func(t *testing.T) {
		s := testcore.NewEnv(t)

		if s.HttpAPIAddress() == "" {
			t.Skip("HTTP API server not enabled")
		}

		h := &httpAPITestHelper{
			t:              t,
			httpAPIAddress: s.HttpAPIAddress(),
			namespace:      s.Namespace().String(),
		}

		_, respBody := h.httpGet(
			http.StatusOK,
			"/openapi.yaml",
			"",
		)
		var spec map[string]interface{}
		// We're not going to validate it here, just verify that it's valid
		s.NoError(yaml.Unmarshal(respBody, &spec), string(respBody))
	})
}

func runHTTPAPIBasicsTest_Protojson(t *testing.T, h *httpAPITestHelper, worker sdkworker.Worker, taskQueue string, capture *metrics.CapturedRecordings, contentType string, pretty bool) {
	// These are callbacks because the worker needs to be initialized so we can get the task queue
	reqBody := func() string {
		requestBody, err := protojson.Marshal(&workflowservice.StartWorkflowExecutionRequest{
			WorkflowType: &commonpb.WorkflowType{Name: "http-basic-workflow"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
			Input:        jsonPayload(`{ "someField": "workflow-arg" }`),
		})
		if err != nil {
			t.Fatalf("Failed to marshal request: %v", err)
		}
		return string(requestBody)
	}
	queryBody := func() string {
		qb, err := protojson.Marshal(&workflowservice.QueryWorkflowRequest{
			Query: &querypb.WorkflowQuery{
				QueryArgs: jsonPayload(`{ "someField": "query-arg" }`),
			},
		})
		if err != nil {
			t.Fatalf("Failed to marshal query body: %v", err)
		}
		return string(qb)
	}
	signalBody := func() string {
		sb, err := protojson.Marshal(&workflowservice.SignalWorkflowExecutionRequest{
			Input: jsonPayload(`{ "someField": "signal-arg" }`),
		})
		if err != nil {
			t.Fatalf("Failed to marshal signal body: %v", err)
		}
		return string(sb)
	}
	verifyQueryResult := func(t *testing.T, respBody []byte) {
		t.Log(string(respBody))
		if pretty {
			// This is lazy but it'll work
			if !strings.Contains(string(respBody), "\n") {
				t.Fatal("Response body should have been prettified")
			}
		}
		var queryResp workflowservice.QueryWorkflowResponse
		if err := protojson.Unmarshal(respBody, &queryResp); err != nil {
			t.Fatalf("Failed to unmarshal query response: %v, body: %s", err, string(respBody))
		}
		if len(queryResp.QueryResult.Payloads) != 1 {
			t.Fatalf("Expected 1 payload, got %d", len(queryResp.QueryResult.Payloads))
		}
		var payload SomeJSONStruct
		conv := converter.NewJSONPayloadConverter()
		if err := conv.FromPayload(queryResp.QueryResult.Payloads[0], &payload); err != nil {
			t.Fatalf("Failed to convert payload: %v", err)
		}
		if payload.SomeField != "query-arg" {
			t.Fatalf("Expected 'query-arg', got: %s", payload.SomeField)
		}
	}
	verifyHistory := func(t *testing.T, respBody []byte) {
		t.Log(string(respBody))
		if pretty {
			// This is lazy but it'll work
			if !strings.Contains(string(respBody), "\n") {
				t.Fatal("Response body should have been prettified")
			}
		}
		var histResp workflowservice.GetWorkflowExecutionHistoryResponse
		if err := protojson.Unmarshal(respBody, &histResp); err != nil {
			t.Fatalf("Failed to unmarshal history response: %v", err)
		}
		if len(histResp.History.Events) != 1 {
			t.Fatalf("Expected 1 event, got %d", len(histResp.History.Events))
		}
		if histResp.History.Events[0].EventType != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
			t.Fatalf("Expected COMPLETED event, got: %v", histResp.History.Events[0].EventType)
		}

		event := histResp.History.Events[0].GetWorkflowExecutionCompletedEventAttributes()
		var payload SomeJSONStruct
		conv := converter.NewJSONPayloadConverter()
		if len(event.Result.Payloads) != 1 {
			t.Fatalf("Expected 1 result payload, got %d", len(event.Result.Payloads))
		}
		if err := conv.FromPayload(event.Result.Payloads[0], &payload); err != nil {
			t.Fatalf("Failed to convert payload: %v", err)
		}
		if payload.SomeField != "workflow-arg" {
			t.Fatalf("Expected 'workflow-arg', got: %s", payload.SomeField)
		}
	}
	runHTTPAPIBasicsTest(t, h, worker, taskQueue, capture, contentType, reqBody, queryBody, signalBody, verifyQueryResult, verifyHistory)
}

func runHTTPAPIBasicsTest_Shorthand(t *testing.T, h *httpAPITestHelper, worker sdkworker.Worker, taskQueue string, capture *metrics.CapturedRecordings, contentType string, pretty bool) {
	reqBody := func() string {
		return `{
				"workflowType": { "name": "http-basic-workflow" },
                "taskQueue": { "name": "` + taskQueue + `" },
                "input": [{ "someField": "workflow-arg" }]
		}`
	}
	queryBody := func() string {
		return `{ "query": { "queryArgs": [{ "someField": "query-arg" }] } }`
	}
	signalBody := func() string {
		return `{ "input": [{ "someField": "signal-arg" }] }`
	}
	verifyQueryResult := func(t *testing.T, respBody []byte) {
		if pretty {
			// This is lazy but it'll work
			if !strings.Contains(string(respBody), "\n") {
				t.Fatal("Response body should have been prettified")
			}
		}
		var queryResp struct {
			QueryResult json.RawMessage `json:"queryResult"`
		}
		if err := json.Unmarshal(respBody, &queryResp); err != nil {
			t.Fatalf("Failed to unmarshal query response: %v", err)
		}
		expected := `[{ "someField": "query-arg" }]`
		// Compare JSON
		var exp, got interface{}
		if err := json.Unmarshal([]byte(expected), &exp); err != nil {
			t.Fatalf("Failed to unmarshal expected: %v", err)
		}
		if err := json.Unmarshal(queryResp.QueryResult, &got); err != nil {
			t.Fatalf("Failed to unmarshal actual: %v", err)
		}
		expBytes, _ := json.Marshal(exp)
		gotBytes, _ := json.Marshal(got)
		if string(expBytes) != string(gotBytes) {
			t.Fatalf("JSON mismatch: expected %s, got %s", expBytes, gotBytes)
		}
	}
	verifyHistory := func(t *testing.T, respBody []byte) {
		if pretty {
			// This is lazy but it'll work
			if !strings.Contains(string(respBody), "\n") {
				t.Fatal("Response body should have been prettified")
			}
		}
		var histResp struct {
			History struct {
				Events []struct {
					EventType                                 string `json:"eventType"`
					WorkflowExecutionCompletedEventAttributes struct {
						Result json.RawMessage `json:"result"`
					} `json:"workflowExecutionCompletedEventAttributes"`
				} `json:"events"`
			} `json:"history"`
		}
		if err := json.Unmarshal(respBody, &histResp); err != nil {
			t.Fatalf("Failed to unmarshal history response: %v", err)
		}
		if histResp.History.Events[0].EventType != "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED" {
			t.Fatalf("Expected COMPLETED event, got: %s", histResp.History.Events[0].EventType)
		}
		expected := `[{ "someField": "workflow-arg" }]`
		// Compare JSON
		var exp, got interface{}
		if err := json.Unmarshal([]byte(expected), &exp); err != nil {
			t.Fatalf("Failed to unmarshal expected: %v", err)
		}
		if err := json.Unmarshal(histResp.History.Events[0].WorkflowExecutionCompletedEventAttributes.Result, &got); err != nil {
			t.Fatalf("Failed to unmarshal actual: %v", err)
		}
		expBytes, _ := json.Marshal(exp)
		gotBytes, _ := json.Marshal(got)
		if string(expBytes) != string(gotBytes) {
			t.Fatalf("JSON mismatch: expected %s, got %s", expBytes, gotBytes)
		}
	}
	runHTTPAPIBasicsTest(t, h, worker, taskQueue, capture, contentType, reqBody, queryBody, signalBody, verifyQueryResult, verifyHistory)
}
