package tests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
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
	testcore.FunctionalTestBase
}

func TestHttpApiTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(HttpApiTestSuite))
}

func (s *HttpApiTestSuite) runHTTPAPIBasicsTest(
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
	s.Worker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: "http-basic-workflow"})

	// Capture metrics
	capture := s.GetTestCluster().Host().CaptureMetricsHandler().StartCapture()

	defer s.GetTestCluster().Host().CaptureMetricsHandler().StopCapture(capture)

	// Start
	workflowID := testcore.RandomizeStr("wf")
	_, respBody := s.httpPost(http.StatusOK, "/namespaces/"+s.Namespace().String()+"/workflows/"+workflowID, contentType, startWFRequestBody())
	var startResp struct {
		RunID string `json:"runId"`
	}
	s.Require().NoError(json.Unmarshal(respBody, &startResp))

	// Check that there is a an HTTP call metric with the proper tags/value. We
	// can't test overall counts because the metrics handler is shared across
	// concurrently executing tests.
	var found bool
	for _, metric := range capture.Snapshot()[metrics.HTTPServiceRequests.Name()] {
		found =
			metric.Tags[metrics.OperationTagName] == "/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution" &&
				metric.Tags["namespace"] == s.Namespace().String() &&
				metric.Value == int64(1)
		if found {
			break
		}
	}
	s.Require().True(found)

	// Confirm already exists error with details and proper code
	_, respBody = s.httpPost(http.StatusConflict, "/namespaces/"+s.Namespace().String()+"/workflows/"+workflowID, contentType, startWFRequestBody())
	var errResp struct {
		Message string `json:"message"`
		Details []struct {
			RunID string `json:"runId"`
		} `json:"details"`
	}
	s.Require().NoError(json.Unmarshal(respBody, &errResp))
	s.Require().Contains(errResp.Message, "already running")
	s.Require().Equal(startResp.RunID, errResp.Details[0].RunID)

	// Query
	_, respBody = s.httpPost(
		http.StatusOK,
		"/namespaces/"+s.Namespace().String()+"/workflows/"+workflowID+"/query/some-query",
		contentType,
		queryBody(),
	)
	verifyQueryResult(s, respBody)

	// Signal which also completes the workflow
	s.httpPost(
		http.StatusOK,
		"/namespaces/"+s.Namespace().String()+"/workflows/"+workflowID+"/signal/some-signal",
		contentType,
		signalBody(),
	)

	// Confirm workflow complete
	_, respBody = s.httpGet(
		http.StatusOK,
		// Our version of gRPC gateway only supports integer enums in queries :-(
		"/namespaces/"+s.Namespace().String()+"/workflows/"+workflowID+"/history?historyEventFilterType=2",
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
	if s.HttpAPIAddress() == "" {
		s.T().Skip("HTTP API server not enabled")
	}
	// These are callbacks because the worker needs to be initialized so we can get the task queue
	reqBody := func() string {
		requestBody, err := protojson.Marshal(&workflowservice.StartWorkflowExecutionRequest{
			WorkflowType: &commonpb.WorkflowType{Name: "http-basic-workflow"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: s.TaskQueue()},
			Input:        jsonPayload(`{ "someField": "workflow-arg" }`),
		})
		s.Require().NoError(err)
		return string(requestBody)
	}
	queryBody := func() string {
		queryBody, err := protojson.Marshal(&workflowservice.QueryWorkflowRequest{
			Query: &querypb.WorkflowQuery{
				QueryArgs: jsonPayload(`{ "someField": "query-arg" }`),
			},
		})
		s.Require().NoError(err)
		return string(queryBody)
	}
	signalBody := func() string {
		signalBody, err := protojson.Marshal(&workflowservice.SignalWorkflowExecutionRequest{
			Input: jsonPayload(`{ "someField": "signal-arg" }`),
		})
		s.Require().NoError(err)
		return string(signalBody)
	}
	verifyQueryResult := func(s *HttpApiTestSuite, respBody []byte) {
		s.T().Log(string(respBody))
		if pretty {
			// This is lazy but it'll work
			s.Require().Contains(respBody, byte('\n'), "Response body should have been prettified")
		}
		var queryResp workflowservice.QueryWorkflowResponse
		s.Require().NoError(protojson.Unmarshal(respBody, &queryResp), string(respBody))
		s.Require().Len(queryResp.QueryResult.Payloads, 1)
		var payload SomeJSONStruct
		conv := converter.NewJSONPayloadConverter()
		s.Require().NoError(conv.FromPayload(queryResp.QueryResult.Payloads[0], &payload))
		s.Require().Equal("query-arg", payload.SomeField)
	}
	verifyHistory := func(s *HttpApiTestSuite, respBody []byte) {
		s.T().Log(string(respBody))
		if pretty {
			// This is lazy but it'll work
			s.Require().Contains(respBody, byte('\n'), "Response body should have been prettified")
		}
		var histResp workflowservice.GetWorkflowExecutionHistoryResponse
		s.Require().NoError(protojson.Unmarshal(respBody, &histResp))
		s.Require().Len(histResp.History.Events, 1)
		s.Require().Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, histResp.History.Events[0].EventType)

		event := histResp.History.Events[0].GetWorkflowExecutionCompletedEventAttributes()
		var payload SomeJSONStruct
		conv := converter.NewJSONPayloadConverter()
		s.Require().Len(event.Result.Payloads, 1)
		s.Require().NoError(conv.FromPayload(event.Result.Payloads[0], &payload))
		s.Require().Equal("workflow-arg", payload.SomeField)
	}
	s.runHTTPAPIBasicsTest(contentType, reqBody, queryBody, signalBody, verifyQueryResult, verifyHistory)
}

func (s *HttpApiTestSuite) runHTTPAPIBasicsTest_Shorthand(contentType string, pretty bool) {
	if s.HttpAPIAddress() == "" {
		s.T().Skip("HTTP API server not enabled")
	}

	reqBody := func() string {
		return `{
				"workflowType": { "name": "http-basic-workflow" },
                "taskQueue": { "name": "` + s.TaskQueue() + `" },
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
			s.Require().Contains(respBody, byte('\n'), "Response body should have been prettified")
		}
		var queryResp struct {
			QueryResult json.RawMessage `json:"queryResult"`
		}
		s.Require().NoError(json.Unmarshal(respBody, &queryResp))
		s.Require().JSONEq(`[{ "someField": "query-arg" }]`, string(queryResp.QueryResult))
	}
	verifyHistory := func(s *HttpApiTestSuite, respBody []byte) {
		if pretty {
			// This is lazy but it'll work
			s.Require().Contains(respBody, byte('\n'), "Response body should have been prettified")
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
		s.Require().NoError(json.Unmarshal(respBody, &histResp))
		s.Require().Equal("EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED", histResp.History.Events[0].EventType)
		s.Require().JSONEq(
			`[{ "someField": "workflow-arg" }]`,
			string(histResp.History.Events[0].WorkflowExecutionCompletedEventAttributes.Result),
		)
	}
	s.runHTTPAPIBasicsTest(contentType, reqBody, queryBody, signalBody, verifyQueryResult, verifyHistory)
}

func (s *HttpApiTestSuite) TestHTTPHostValidation() {
	s.OverrideDynamicConfig(dynamicconfig.FrontendHTTPAllowedHosts, []string{"allowed"})
	{
		req, err := http.NewRequest("GET", "/system-info", nil)
		s.Require().NoError(err)
		req.Host = "allowed"
		req.Header.Add("Accept", "application/json")
		req.Header.Add("Content-Type", "application/json")
		s.httpRequest(http.StatusOK, req)
	}
	{
		req, err := http.NewRequest("GET", "/system-info", nil)
		s.Require().NoError(err)
		req.Host = "not-allowed"
		req.Header.Add("Accept", "application/json")
		req.Header.Add("Content-Type", "application/json")
		s.httpRequest(http.StatusForbidden, req)
	}
}

func (s *HttpApiTestSuite) TestHTTPAPIHeaders() {
	if s.HttpAPIAddress() == "" {
		s.T().Skip("HTTP API server not enabled")
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
	s.Require().NoError(err)
	req.Header.Set("Authorization", "my-auth-token")
	req.Header.Set("X-Forwarded-For", "1.2.3.4:5678")
	// These headers are set to forward deep in the onebox config
	req.Header.Set("This-Header-Forwarded", "some-value")
	req.Header.Set("This-Header-Prefix-Forwarded-Foo", "foo")
	req.Header.Set("This-Header-Prefix-Forwarded-Bar", "bar")
	req.Header.Set("This-Header-Not-Forwarded", "some-value")
	s.httpRequest(http.StatusOK, req)

	// Confirm the claims got my auth token
	callbackLock.RLock()
	defer callbackLock.RUnlock()
	s.Require().Equal("my-auth-token", lastInfo.AuthToken)

	// Check headers
	s.Require().Equal("my-auth-token", listWorkflowMetadata["authorization"][0])
	s.Require().Contains(listWorkflowMetadata["x-forwarded-for"][0], "1.2.3.4:5678")
	s.Require().Equal("some-value", listWorkflowMetadata["this-header-forwarded"][0])
	s.Require().Equal("foo", listWorkflowMetadata["this-header-prefix-forwarded-foo"][0])
	s.Require().Equal("bar", listWorkflowMetadata["this-header-prefix-forwarded-bar"][0])
	s.Require().NotContains(listWorkflowMetadata, "this-header-not-forwarded")
	s.Require().Equal(headers.ClientNameServerHTTP, listWorkflowMetadata[headers.ClientNameHeaderName][0])
	s.Require().Equal(headers.ServerVersion, listWorkflowMetadata[headers.ClientVersionHeaderName][0])
}

func (s *HttpApiTestSuite) TestHTTPAPIPretty() {
	if s.HttpAPIAddress() == "" {
		s.T().Skip("HTTP API server not enabled")
	}
	// Make a call to system info normal, confirm no newline, then ask for pretty
	// and confirm newlines
	_, b := s.httpGet(http.StatusOK, "/system-info", "application/json")
	s.Require().NotContains(b, byte('\n'))
	_, b = s.httpGet(http.StatusOK, "/system-info?pretty", "application/json")
	s.Require().Contains(b, byte('\n'))
}

func (s *HttpApiTestSuite) httpGet(expectedStatus int, url, contentType string) (*http.Response, []byte) {
	req, err := http.NewRequest("GET", url, nil)
	s.Require().NoError(err)
	if contentType != "" {
		req.Header.Add("Accept", contentType)
		req.Header.Add("Content-Type", contentType)
	}
	s.T().Logf("GET %s (Accept: %s)", url, contentType)
	return s.httpRequest(expectedStatus, req)
}

func (s *HttpApiTestSuite) httpPost(expectedStatus int, url, contentType, jsonBody string) (*http.Response, []byte) {
	req, err := http.NewRequest("POST", url, strings.NewReader(jsonBody))
	s.Require().NoError(err)
	req.Header.Add("Accept", contentType)
	req.Header.Add("Content-Type", contentType)
	s.T().Logf("POST %s (Accept: %s)", url, contentType)
	return s.httpRequest(expectedStatus, req)
}

func (s *HttpApiTestSuite) httpRequest(expectedStatus int, req *http.Request) (*http.Response, []byte) {
	if req.URL.Scheme == "" {
		req.URL.Scheme = "http"
	}
	if req.URL.Host == "" {
		req.URL.Host = s.HttpAPIAddress()
	}
	resp, err := http.DefaultClient.Do(req)
	s.Require().NoError(err)
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	s.Require().NoError(err)
	s.Require().Equal(expectedStatus, resp.StatusCode, "Bad status, body: %s", body)
	return resp, body
}

func (s *HttpApiTestSuite) TestHTTPAPI_OperatorService_ListSearchAttributes() {
	_, respBody := s.httpGet(
		http.StatusOK,
		"/cluster/namespaces/"+s.Namespace().String()+"/search-attributes",
		"application/json",
	)
	s.T().Log(string(respBody))
	var searchAttrsResp struct {
		CustomAttributes map[string]string `json:"customAttributes"`
		SystemAttributes map[string]string `json:"systemAttributes"`
		StorageSchema    map[string]string `json:"storageSchema"`
	}
	s.Require().NoError(json.Unmarshal(respBody, &searchAttrsResp))
	// We don't allow for creating search attributes from the HTTP API yet, so
	// we just check that a few defaults exist. We don't want to check for all
	// of them as that's brittle and will break the tests if we ever add a new type
	s.Require().Contains(searchAttrsResp.CustomAttributes, "CustomIntField")
	s.Require().Equal(searchAttrsResp.CustomAttributes["CustomIntField"], "INDEXED_VALUE_TYPE_INT")
}

func (s *HttpApiTestSuite) TestHTTPAPI_Serves_OpenAPIv2_Docs() {
	_, respBody := s.httpGet(
		http.StatusOK,
		"/swagger.json",
		"",
	)
	var spec map[string]interface{}
	// We're not going to validate it here, just verify that it's valid
	s.Require().NoError(json.Unmarshal(respBody, &spec), string(respBody))
}

func (s *HttpApiTestSuite) TestHTTPAPI_Serves_OpenAPIv3_Docs() {
	_, respBody := s.httpGet(
		http.StatusOK,
		"/openapi.yaml",
		"",
	)
	var spec map[string]interface{}
	// We're not going to validate it here, just verify that it's valid
	s.Require().NoError(yaml.Unmarshal(respBody, &spec), string(respBody))
}
