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
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	"gopkg.in/yaml.v3"

	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/query/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
)

type SomeJSONStruct struct {
	SomeField string `json:"someField"`
}

func jsonPayload(data string) *common.Payloads {
	return &common.Payloads{
		Payloads: []*common.Payload{{
			Metadata: map[string][]byte{
				converter.MetadataEncoding: []byte(converter.MetadataEncodingJSON),
			},
			Data: []byte(data),
		}},
	}
}

func (s *ClientFunctionalSuite) runHTTPAPIBasicsTest(
	contentType string,
	startWFRequestBody, queryBody, signalBody func() string,
	verifyQueryResult, verifyHistory func(*ClientFunctionalSuite, []byte)) {
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
	s.worker.RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: "http-basic-workflow"})

	// Capture metrics
	capture := s.testCluster.host.captureMetricsHandler.StartCapture()
	defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)

	// Start
	workflowID := s.randomizeStr("wf")
	_, respBody := s.httpPost(http.StatusOK, "/namespaces/"+s.namespace+"/workflows/"+workflowID, contentType, startWFRequestBody())
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
				metric.Tags["namespace"] == s.namespace &&
				metric.Value == int64(1)
		if found {
			break
		}
	}
	s.Require().True(found)

	// Confirm already exists error with details and proper code
	_, respBody = s.httpPost(http.StatusConflict, "/namespaces/"+s.namespace+"/workflows/"+workflowID, contentType, startWFRequestBody())
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
		"/namespaces/"+s.namespace+"/workflows/"+workflowID+"/query/some-query",
		contentType,
		queryBody(),
	)
	verifyQueryResult(s, respBody)

	// Signal which also completes the workflow
	s.httpPost(
		http.StatusOK,
		"/namespaces/"+s.namespace+"/workflows/"+workflowID+"/signal/some-signal",
		contentType,
		signalBody(),
	)

	// Confirm workflow complete
	_, respBody = s.httpGet(
		http.StatusOK,
		// Our version of gRPC gateway only supports integer enums in queries :-(
		"/namespaces/"+s.namespace+"/workflows/"+workflowID+"/history?historyEventFilterType=2",
		contentType,
	)
	verifyHistory(s, respBody)
}

func (s *ClientFunctionalSuite) TestHTTPAPIBasics_Protojson() {
	s.runHTTPAPIBasicsTest_Protojson("application/json+no-payload-shorthand", false)
}

func (s *ClientFunctionalSuite) TestHTTPAPIBasics_ProtojsonPretty() {
	s.runHTTPAPIBasicsTest_Protojson("application/json+pretty+no-payload-shorthand", true)
}

func (s *ClientFunctionalSuite) TestHTTPAPIBasics_Shorthand() {
	s.runHTTPAPIBasicsTest_Shorthand("application/json", false)
}

func (s *ClientFunctionalSuite) TestHTTPAPIBasics_ShorthandPretty() {
	s.runHTTPAPIBasicsTest_Shorthand("application/json+pretty", true)
}

func (s *ClientFunctionalSuite) runHTTPAPIBasicsTest_Protojson(contentType string, pretty bool) {
	if s.httpAPIAddress == "" {
		s.T().Skip("HTTP API server not enabled")
	}
	// These are callbacks because the worker needs to be initialized so we can get the task queue
	reqBody := func() string {
		requestBody, err := protojson.Marshal(&workflowservice.StartWorkflowExecutionRequest{
			WorkflowType: &common.WorkflowType{Name: "http-basic-workflow"},
			TaskQueue:    &taskqueue.TaskQueue{Name: s.taskQueue},
			Input:        jsonPayload(`{ "someField": "workflow-arg" }`),
		})
		s.Require().NoError(err)
		return string(requestBody)
	}
	queryBody := func() string {
		queryBody, err := protojson.Marshal(&workflowservice.QueryWorkflowRequest{
			Query: &query.WorkflowQuery{
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
	verifyQueryResult := func(s *ClientFunctionalSuite, respBody []byte) {
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
	verifyHistory := func(s *ClientFunctionalSuite, respBody []byte) {
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

func (s *ClientFunctionalSuite) runHTTPAPIBasicsTest_Shorthand(contentType string, pretty bool) {
	if s.httpAPIAddress == "" {
		s.T().Skip("HTTP API server not enabled")
	}

	reqBody := func() string {
		return `{
				"workflowType": { "name": "http-basic-workflow" },
                "taskQueue": { "name": "` + s.taskQueue + `" },
                "input": [{ "someField": "workflow-arg" }]
		}`
	}
	queryBody := func() string {
		return `{ "query": { "queryArgs": [{ "someField": "query-arg" }] } }`
	}
	signalBody := func() string {
		return `{ "input": [{ "someField": "signal-arg" }] }`
	}
	verifyQueryResult := func(s *ClientFunctionalSuite, respBody []byte) {
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
	verifyHistory := func(s *ClientFunctionalSuite, respBody []byte) {
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

func (s *ClientFunctionalSuite) TestHTTPAPIHeaders() {
	if s.httpAPIAddress == "" {
		s.T().Skip("HTTP API server not enabled")
	}
	// Make a claim mapper and authorizer that capture info
	var lastInfo *authorization.AuthInfo
	var listWorkflowMetadata metadata.MD
	var callbackLock sync.RWMutex
	s.testCluster.host.SetOnGetClaims(func(info *authorization.AuthInfo) (*authorization.Claims, error) {
		callbackLock.Lock()
		defer callbackLock.Unlock()
		if info != nil {
			lastInfo = info
		}
		return &authorization.Claims{System: authorization.RoleAdmin}, nil
	})
	s.testCluster.host.SetOnAuthorize(func(
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
	req, err := http.NewRequest("GET", "/namespaces/"+s.namespace+"/workflows", nil)
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

func (s *ClientFunctionalSuite) TestHTTPAPIPretty() {
	if s.httpAPIAddress == "" {
		s.T().Skip("HTTP API server not enabled")
	}
	// Make a call to system info normal, confirm no newline, then ask for pretty
	// and confirm newlines
	_, b := s.httpGet(http.StatusOK, "/system-info", "application/json")
	s.Require().NotContains(b, byte('\n'))
	_, b = s.httpGet(http.StatusOK, "/system-info?pretty", "application/json")
	s.Require().Contains(b, byte('\n'))
}

func (s *ClientFunctionalSuite) httpGet(expectedStatus int, url, contentType string) (*http.Response, []byte) {
	req, err := http.NewRequest("GET", url, nil)
	s.Require().NoError(err)
	if contentType != "" {
		req.Header.Add("Accept", contentType)
		req.Header.Add("Content-Type", contentType)
	}
	s.T().Logf("GET %s (Accept: %s)", url, contentType)
	return s.httpRequest(expectedStatus, req)
}

func (s *ClientFunctionalSuite) httpPost(expectedStatus int, url, contentType, jsonBody string) (*http.Response, []byte) {
	req, err := http.NewRequest("POST", url, strings.NewReader(jsonBody))
	s.Require().NoError(err)
	req.Header.Add("Accept", contentType)
	req.Header.Add("Content-Type", contentType)
	s.T().Logf("POST %s (Accept: %s)", url, contentType)
	return s.httpRequest(expectedStatus, req)
}

func (s *ClientFunctionalSuite) httpRequest(expectedStatus int, req *http.Request) (*http.Response, []byte) {
	if req.URL.Scheme == "" {
		req.URL.Scheme = "http"
	}
	if req.URL.Host == "" {
		req.URL.Host = s.httpAPIAddress
	}
	resp, err := http.DefaultClient.Do(req)
	s.Require().NoError(err)
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	s.Require().NoError(err)
	s.Require().Equal(expectedStatus, resp.StatusCode, "Bad status, body: %s", body)
	return resp, body
}

func (s *ClientFunctionalSuite) TestHTTPAPI_OperatorService_ListSearchAttributes() {
	_, respBody := s.httpGet(
		http.StatusOK,
		"/cluster/namespaces/"+s.namespace+"/search-attributes",
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

func (s *ClientFunctionalSuite) TestHTTPAPI_Serves_OpenAPIv2_Docs() {
	_, respBody := s.httpGet(
		http.StatusOK,
		"/swagger.json",
		"",
	)
	var spec map[string]interface{}
	// We're not going to validate it here, just verify that it's valid
	s.Require().NoError(json.Unmarshal(respBody, &spec), string(respBody))
}

func (s *ClientFunctionalSuite) TestHTTPAPI_Serves_OpenAPIv3_Docs() {
	_, respBody := s.httpGet(
		http.StatusOK,
		"/openapi.yaml",
		"",
	)
	var spec map[string]interface{}
	// We're not going to validate it here, just verify that it's valid
	s.Require().NoError(yaml.Unmarshal(respBody, &spec), string(respBody))
}
