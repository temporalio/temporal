package tests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc/metadata"
)

type SomeJSONStruct struct {
	SomeField string `json:"someField"`
}

func (s *clientIntegrationSuite) TestHTTPAPIBasics() {
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

	// Start
	workflowID := s.randomizeStr("wf")
	_, respBody := s.httpPost(http.StatusOK, "/api/v1/namespaces/"+s.namespace+"/workflows/"+workflowID, `{
		"workflowType": { "name": "http-basic-workflow" },
		"taskQueue": { "name": "`+s.taskQueue+`" },
		"input": [{ "someField": "workflow-arg" }],
		"requestId": "`+s.randomizeStr("req")+`"
	}`)
	var startResp struct {
		RunID string `json:"runId"`
	}
	s.Require().NoError(json.Unmarshal(respBody, &startResp))

	// Check that our single HTTP call metric is present with the proper tags
	httpMetrics := s.testCluster.host.captureMetricsHandler.Snapshot()[metrics.HTTPServiceRequests.GetMetricName()]
	s.Require().Len(httpMetrics, 1)
	s.Require().Equal(int64(1), httpMetrics[0].Value)
	s.Require().Equal(
		"/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution",
		httpMetrics[0].Tags[metrics.OperationTagName],
	)
	s.Require().Equal(s.namespace, httpMetrics[0].Tags["namespace"])

	// Confirm already exists error with details and proper code
	_, respBody = s.httpPost(http.StatusConflict, "/api/v1/namespaces/"+s.namespace+"/workflows/"+workflowID, `{
		"workflowType": { "name": "http-basic-workflow" },
		"taskQueue": { "name": "`+s.taskQueue+`" },
		"input": [{ "someField": "workflow-arg" }],
		"requestId": "`+s.randomizeStr("req")+`"
	}`)
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
		"/api/v1/namespaces/"+s.namespace+"/workflows/"+workflowID+"/query/some-query",
		`{ "query": { "queryArgs": [{ "someField": "query-arg" }] } }`,
	)
	var queryResp struct {
		QueryResult json.RawMessage `json:"queryResult"`
	}
	s.Require().NoError(json.Unmarshal(respBody, &queryResp))
	s.Require().JSONEq(`[{ "someField": "query-arg" }]`, string(queryResp.QueryResult))

	// Signal which also completes the workflow
	s.httpPost(
		http.StatusOK,
		"/api/v1/namespaces/"+s.namespace+"/workflows/"+workflowID+"/signal/some-signal",
		`{ "input": [{ "someField": "signal-arg" }] }`,
	)

	// Confirm workflow complete
	_, respBody = s.httpGet(
		http.StatusOK,
		// Our version of gRPC gateway only supports integer enums in queries :-(
		"/api/v1/namespaces/"+s.namespace+"/workflows/"+workflowID+"/history?historyEventFilterType=2",
	)
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
	s.Require().Equal("WorkflowExecutionCompleted", histResp.History.Events[0].EventType)
	s.Require().JSONEq(
		`[{ "someField": "workflow-arg" }]`,
		string(histResp.History.Events[0].WorkflowExecutionCompletedEventAttributes.Result),
	)

}

func (s *clientIntegrationSuite) TestHTTPAPIHeaders() {
	// Make a claim mapper and authorizer that capture info
	var lastInfo *authorization.AuthInfo
	var listWorkflowMetadata metadata.MD
	s.testCluster.host.onGetClaims = func(info *authorization.AuthInfo) (*authorization.Claims, error) {
		if info != nil {
			lastInfo = info
		}
		return &authorization.Claims{System: authorization.RoleAdmin}, nil
	}
	s.testCluster.host.onAuthorize = func(
		ctx context.Context,
		caller *authorization.Claims,
		target *authorization.CallTarget,
	) (authorization.Result, error) {
		if target.APIName == "/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions" {
			listWorkflowMetadata, _ = metadata.FromIncomingContext(ctx)
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	}

	// Make a simple list call that we don't care about the result
	req, err := http.NewRequest("GET", "/api/v1/namespaces/"+s.namespace+"/workflows", nil)
	s.Require().NoError(err)
	req.Header.Set("Authorization", "my-auth-token")
	req.Header.Set("X-Forwarded-For", "1.2.3.4:5678")
	// The header is set to forward deep in the onebox config
	req.Header.Set("This-Header-Forwarded", "some-value")
	req.Header.Set("This-Header-Not-Forwarded", "some-value")
	s.httpRequest(http.StatusOK, req)

	// Confirm the claims got my auth token
	s.Require().Equal("my-auth-token", lastInfo.AuthToken)

	// Check headers
	s.Require().Equal("my-auth-token", listWorkflowMetadata["authorization"][0])
	s.Require().Contains(listWorkflowMetadata["x-forwarded-for"][0], "1.2.3.4:5678")
	s.Require().Equal("some-value", listWorkflowMetadata["this-header-forwarded"][0])
	s.Require().NotContains(listWorkflowMetadata, "this-header-not-forwarded")
	s.Require().Equal(headers.ClientNameServerHTTP, listWorkflowMetadata[headers.ClientNameHeaderName][0])
	s.Require().Equal(headers.ServerVersion, listWorkflowMetadata[headers.ClientVersionHeaderName][0])
}

func (s *clientIntegrationSuite) TestHTTPAPIPretty() {
	// Make a call to system info normal, confirm no newline, then ask for pretty
	// and confirm newlines
	_, b := s.httpGet(http.StatusOK, "/api/v1/system-info")
	s.Require().NotContains(b, byte('\n'))
	_, b = s.httpGet(http.StatusOK, "/api/v1/system-info?pretty")
	s.Require().Contains(b, byte('\n'))
}

func (s *clientIntegrationSuite) httpGet(expectedStatus int, url string) (*http.Response, []byte) {
	req, err := http.NewRequest("GET", url, nil)
	s.Require().NoError(err)
	return s.httpRequest(expectedStatus, req)
}

func (s *clientIntegrationSuite) httpPost(expectedStatus int, url string, jsonBody string) (*http.Response, []byte) {
	req, err := http.NewRequest("POST", url, strings.NewReader(jsonBody))
	s.Require().NoError(err)
	req.Header.Set("Content-Type", "application/json")
	return s.httpRequest(expectedStatus, req)
}

func (s *clientIntegrationSuite) httpRequest(expectedStatus int, req *http.Request) (*http.Response, []byte) {
	if req.URL.Scheme == "" {
		req.URL.Scheme = "http"
	}
	if req.URL.Host == "" {
		req.URL.Host = s.httpAPIAddress
	}
	resp, err := http.DefaultClient.Do(req)
	s.Require().NoError(err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	s.Require().NoError(err)
	s.Require().Equal(expectedStatus, resp.StatusCode, "Bad status, body: %s", body)
	return resp, body
}
