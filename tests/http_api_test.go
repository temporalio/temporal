package tests

import (
	"cmp"
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
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/parallelsuite"
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

type HttpApiTestSuite struct {
	parallelsuite.Suite[*HttpApiTestSuite]
}

func TestHttpApiTestSuite(t *testing.T) {
	parallelsuite.Run(t, &HttpApiTestSuite{})
}

func (s *HttpApiTestSuite) newTestEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	return testcore.NewEnv(s.T(), opts...)
}

func (s *HttpApiTestSuite) runHTTPAPIBasicsTest(
	env *testcore.TestEnv,
	contentType string,
	startWFRequestBody, queryBody, signalBody func() string,
	verifyQueryResult, verifyHistory func(*HttpApiTestSuite, []byte)) {
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
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: "http-basic-workflow"})

	// Capture metrics
	capture := env.StartNamespaceMetricCapture()

	// Start
	workflowID := testcore.RandomizeStr("wf")
	_, respBody := s.httpPost(env, http.StatusOK, "/namespaces/"+env.Namespace().String()+"/workflows/"+workflowID, contentType, startWFRequestBody())
	var startResp struct {
		RunID string `json:"runId"`
	}
	s.NoError(json.Unmarshal(respBody, &startResp))

	// Check that there is a an HTTP call metric with the proper tags/value. We
	// can't test overall counts because the metrics handler is shared across
	// concurrently executing tests.
	var found bool
	for _, metric := range capture.Metric(metrics.HTTPServiceRequests.Name()) {
		found =
			metric.Tags[metrics.OperationTagName] == "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution" &&
				metric.Tags["namespace"] == env.Namespace().String() &&
				metric.Value == int64(1)
		if found {
			break
		}
	}
	s.True(found)

	// Confirm already exists error with details and proper code
	_, respBody = s.httpPost(env, http.StatusConflict, "/namespaces/"+env.Namespace().String()+"/workflows/"+workflowID, contentType, startWFRequestBody())
	var errResp struct {
		Message string `json:"message"`
		Details []struct {
			RunID string `json:"runId"`
		} `json:"details"`
	}
	s.NoError(json.Unmarshal(respBody, &errResp))
	s.Contains(errResp.Message, "already running")
	s.Equal(startResp.RunID, errResp.Details[0].RunID)

	// Query
	_, respBody = s.httpPost(
		env,
		http.StatusOK,
		"/namespaces/"+env.Namespace().String()+"/workflows/"+workflowID+"/query/some-query",
		contentType,
		queryBody(),
	)
	verifyQueryResult(s, respBody)

	// Signal which also completes the workflow
	s.httpPost(
		env,
		http.StatusOK,
		"/namespaces/"+env.Namespace().String()+"/workflows/"+workflowID+"/signal/some-signal",
		contentType,
		signalBody(),
	)

	// Confirm workflow complete
	_, respBody = s.httpGet(
		env,
		http.StatusOK,
		// Our version of gRPC gateway only supports integer enums in queries :-(
		"/namespaces/"+env.Namespace().String()+"/workflows/"+workflowID+"/history?historyEventFilterType=2",
		contentType,
	)
	verifyHistory(s, respBody)
}

func (s *HttpApiTestSuite) TestHTTPAPIBasics_Protojson() {
	s.runHTTPAPIBasicsTest_Protojson("application/json+no-payload-shorthand", false)
}

func (s *HttpApiTestSuite) TestHTTPAPIBasics_ProtojsonPretty() {
	s.runHTTPAPIBasicsTest_Protojson("application/json+pretty+no-payload-shorthand", true)
}

func (s *HttpApiTestSuite) TestHTTPAPIBasics_Shorthand() {
	s.runHTTPAPIBasicsTest_Shorthand("application/json", false)
}

func (s *HttpApiTestSuite) TestHTTPAPIBasics_ShorthandPretty() {
	s.runHTTPAPIBasicsTest_Shorthand("application/json+pretty", true)
}

func (s *HttpApiTestSuite) runHTTPAPIBasicsTest_Protojson(contentType string, pretty bool) {
	env := s.newTestEnv()
	// These are callbacks because the worker needs to be initialized so we can get the task queue
	reqBody := func() string {
		requestBody, err := protojson.Marshal(&workflowservice.StartWorkflowExecutionRequest{
			WorkflowType: &commonpb.WorkflowType{Name: "http-basic-workflow"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue()},
			Input:        jsonPayload(`{ "someField": "workflow-arg" }`),
		})
		s.NoError(err)
		return string(requestBody)
	}
	queryBody := func() string {
		queryBody, err := protojson.Marshal(&workflowservice.QueryWorkflowRequest{
			Query: &querypb.WorkflowQuery{
				QueryArgs: jsonPayload(`{ "someField": "query-arg" }`),
			},
		})
		s.NoError(err)
		return string(queryBody)
	}
	signalBody := func() string {
		signalBody, err := protojson.Marshal(&workflowservice.SignalWorkflowExecutionRequest{
			Input: jsonPayload(`{ "someField": "signal-arg" }`),
		})
		s.NoError(err)
		return string(signalBody)
	}
	verifyQueryResult := func(s *HttpApiTestSuite, respBody []byte) {
		s.T().Log(string(respBody))
		if pretty {
			// This is lazy but it'll work
			s.Contains(respBody, byte('\n'), "Response body should have been prettified")
		}
		var queryResp workflowservice.QueryWorkflowResponse
		s.NoError(protojson.Unmarshal(respBody, &queryResp), string(respBody))
		s.Len(queryResp.QueryResult.Payloads, 1)
		var payload SomeJSONStruct
		conv := converter.NewJSONPayloadConverter()
		s.NoError(conv.FromPayload(queryResp.QueryResult.Payloads[0], &payload))
		s.Equal("query-arg", payload.SomeField)
	}
	verifyHistory := func(s *HttpApiTestSuite, respBody []byte) {
		s.T().Log(string(respBody))
		if pretty {
			// This is lazy but it'll work
			s.Contains(respBody, byte('\n'), "Response body should have been prettified")
		}
		var histResp workflowservice.GetWorkflowExecutionHistoryResponse
		s.NoError(protojson.Unmarshal(respBody, &histResp))
		s.Len(histResp.History.Events, 1)
		s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, histResp.History.Events[0].EventType)

		event := histResp.History.Events[0].GetWorkflowExecutionCompletedEventAttributes()
		var payload SomeJSONStruct
		conv := converter.NewJSONPayloadConverter()
		s.Len(event.Result.Payloads, 1)
		s.NoError(conv.FromPayload(event.Result.Payloads[0], &payload))
		s.Equal("workflow-arg", payload.SomeField)
	}
	s.runHTTPAPIBasicsTest(env, contentType, reqBody, queryBody, signalBody, verifyQueryResult, verifyHistory)
}

func (s *HttpApiTestSuite) runHTTPAPIBasicsTest_Shorthand(contentType string, pretty bool) {
	env := s.newTestEnv()

	reqBody := func() string {
		return `{
				"workflowType": { "name": "http-basic-workflow" },
                "taskQueue": { "name": "` + env.WorkerTaskQueue() + `" },
                "input": [{ "someField": "workflow-arg" }]
		}`
	}
	queryBody := func() string {
		return `{ "query": { "queryArgs": [{ "someField": "query-arg" }] } }`
	}
	signalBody := func() string {
		return `{ "input": [{ "someField": "signal-arg" }] }`
	}
	verifyQueryResult := func(s *HttpApiTestSuite, respBody []byte) {
		if pretty {
			// This is lazy but it'll work
			s.Contains(respBody, byte('\n'), "Response body should have been prettified")
		}
		var queryResp struct {
			QueryResult json.RawMessage `json:"queryResult"`
		}
		s.NoError(json.Unmarshal(respBody, &queryResp))
		s.JSONEq(`[{ "someField": "query-arg" }]`, string(queryResp.QueryResult))
	}
	verifyHistory := func(s *HttpApiTestSuite, respBody []byte) {
		if pretty {
			// This is lazy but it'll work
			s.Contains(respBody, byte('\n'), "Response body should have been prettified")
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
		s.NoError(json.Unmarshal(respBody, &histResp))
		s.Equal("EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED", histResp.History.Events[0].EventType)
		s.JSONEq(
			`[{ "someField": "workflow-arg" }]`,
			string(histResp.History.Events[0].WorkflowExecutionCompletedEventAttributes.Result),
		)
	}
	s.runHTTPAPIBasicsTest(env, contentType, reqBody, queryBody, signalBody, verifyQueryResult, verifyHistory)
}

func (s *HttpApiTestSuite) TestHTTPHostValidation() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.FrontendHTTPAllowedHosts, []string{"allowed"}))
	{
		req, err := http.NewRequestWithContext(s.Context(), "GET", "/system-info", nil)
		s.NoError(err)
		req.Host = "allowed"
		req.Header.Add("Accept", "application/json")
		req.Header.Add("Content-Type", "application/json")
		s.httpRequest(env, http.StatusOK, req)
	}
	{
		req, err := http.NewRequestWithContext(s.Context(), "GET", "/system-info", nil)
		s.NoError(err)
		req.Host = "not-allowed"
		req.Header.Add("Accept", "application/json")
		req.Header.Add("Content-Type", "application/json")
		s.httpRequest(env, http.StatusForbidden, req)
	}
}

func (s *HttpApiTestSuite) TestHTTPAPIHeaders() {
	env := s.newTestEnv(testcore.WithDedicatedCluster())
	// Make a claim mapper and authorizer that capture info
	var lastInfo *authorization.AuthInfo
	var listWorkflowMetadata metadata.MD
	var callbackLock sync.RWMutex
	env.SetOnGetClaims(func(info *authorization.AuthInfo) (*authorization.Claims, error) {
		callbackLock.Lock()
		defer callbackLock.Unlock()
		if info != nil {
			lastInfo = info
		}
		return &authorization.Claims{System: authorization.RoleAdmin}, nil
	})
	env.SetOnAuthorize(func(
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
	req, err := http.NewRequestWithContext(s.Context(), "GET", "/namespaces/"+env.Namespace().String()+"/workflows", nil)
	s.NoError(err)
	req.Header.Set("Authorization", "my-auth-token")
	req.Header.Set("X-Forwarded-For", "1.2.3.4:5678")
	// These headers are set to forward deep in the onebox config
	req.Header.Set("This-Header-Forwarded", "some-value")
	req.Header.Set("This-Header-Prefix-Forwarded-Foo", "foo")
	req.Header.Set("This-Header-Prefix-Forwarded-Bar", "bar")
	req.Header.Set("This-Header-Not-Forwarded", "some-value")
	s.httpRequest(env, http.StatusOK, req)

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
}

func (s *HttpApiTestSuite) TestHTTPAPIPretty() {
	env := s.newTestEnv()
	// Make a call to system info normal, confirm no newline, then ask for pretty
	// and confirm newlines
	_, b := s.httpGet(env, http.StatusOK, "/system-info", "application/json")
	s.NotContains(b, byte('\n'))
	_, b = s.httpGet(env, http.StatusOK, "/system-info?pretty", "application/json")
	s.Contains(b, byte('\n'))
}

func (s *HttpApiTestSuite) httpGet(env *testcore.TestEnv, expectedStatus int, url, contentType string) (*http.Response, []byte) {
	req, err := http.NewRequestWithContext(s.Context(), "GET", url, nil)
	s.NoError(err)
	if contentType != "" {
		req.Header.Add("Accept", contentType)
		req.Header.Add("Content-Type", contentType)
	}
	s.T().Logf("GET %s (Accept: %s)", url, contentType)
	return s.httpRequest(env, expectedStatus, req)
}

func (s *HttpApiTestSuite) httpPost(env *testcore.TestEnv, expectedStatus int, url, contentType, jsonBody string) (*http.Response, []byte) {
	req, err := http.NewRequestWithContext(s.Context(), "POST", url, strings.NewReader(jsonBody))
	s.NoError(err)
	req.Header.Add("Accept", contentType)
	req.Header.Add("Content-Type", contentType)
	s.T().Logf("POST %s (Accept: %s)", url, contentType)
	return s.httpRequest(env, expectedStatus, req)
}

func (s *HttpApiTestSuite) httpRequest(env *testcore.TestEnv, expectedStatus int, req *http.Request) (*http.Response, []byte) {
	req.URL.Scheme = cmp.Or(req.URL.Scheme, "http")
	req.URL.Host = cmp.Or(req.URL.Host, env.HttpAPIAddress())
	resp, err := http.DefaultClient.Do(req)
	s.NoError(err)
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	s.NoError(err)
	s.Equal(expectedStatus, resp.StatusCode, "Bad status, body: %s", body)
	return resp, body
}

func (s *HttpApiTestSuite) TestHTTPAPI_OperatorService_ListSearchAttributes() {
	env := s.newTestEnv()
	_, respBody := s.httpGet(
		env,
		http.StatusOK,
		"/cluster/namespaces/"+env.Namespace().String()+"/search-attributes",
		"application/json",
	)
	s.T().Log(string(respBody))
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
	s.Equal("INDEXED_VALUE_TYPE_INT", searchAttrsResp.CustomAttributes["CustomIntField"])
}

func (s *HttpApiTestSuite) TestHTTPAPI_Serves_OpenAPIv2_Docs() {
	env := s.newTestEnv()
	_, respBody := s.httpGet(
		env,
		http.StatusOK,
		"/swagger.json",
		"",
	)
	var spec map[string]any
	// We're not going to validate it here, just verify that it's valid
	s.NoError(json.Unmarshal(respBody, &spec), string(respBody))
}

func (s *HttpApiTestSuite) TestHTTPAPI_Serves_OpenAPIv3_Docs() {
	env := s.newTestEnv()
	_, respBody := s.httpGet(
		env,
		http.StatusOK,
		"/openapi.yaml",
		"",
	)
	var spec map[string]any
	// We're not going to validate it here, just verify that it's valid
	s.NoError(yaml.Unmarshal(respBody, &spec), string(respBody))
}
