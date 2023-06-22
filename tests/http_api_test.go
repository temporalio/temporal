package tests

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"go.temporal.io/sdk/workflow"
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

func (s *clientIntegrationSuite) httpGet(expectedStatus int, relUrl string) (*http.Response, []byte) {
	resp, err := http.Get("http://" + s.httpAPIAddress + relUrl)
	s.Require().NoError(err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	s.Require().NoError(err)
	s.Require().Equal(expectedStatus, resp.StatusCode, "Bad status, body: %s", body)
	return resp, body
}

func (s *clientIntegrationSuite) httpPost(expectedStatus int, relUrl string, jsonBody string) (*http.Response, []byte) {
	resp, err := http.Post("http://"+s.httpAPIAddress+relUrl, "application/json", strings.NewReader(jsonBody))
	s.Require().NoError(err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	s.Require().NoError(err)
	s.Require().Equal(expectedStatus, resp.StatusCode, "Bad status, body: %s", body)
	return resp, body
}

/*

TODO(cretz): Tests/impl to write

* TLS support including mTLS
* Header pass through including JWT support
* Version header fallback if not provided
* Pretty JSON
* Payload shorthand disabled

*/
