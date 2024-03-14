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

package routing_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/routing"
)

type MyRouteParams struct {
	Namespace  string
	WorkflowID string
}

func newWorkflowRoute() routing.Route[MyRouteParams] {
	return routing.NewRoute[MyRouteParams](
		routing.Slugs[MyRouteParams]("api", "v1", "namespaces"),
		routing.StringParam("namespace", func(params *MyRouteParams) *string { return &params.Namespace }),
		routing.Slugs[MyRouteParams]("workflows"),
		routing.StringParam("workflowID", func(params *MyRouteParams) *string { return &params.WorkflowID }),
	)
}

func ExampleRoute() {
	route := newWorkflowRoute()
	router := mux.NewRouter()
	router.HandleFunc("/"+route.Representation(), func(w http.ResponseWriter, r *http.Request) {
		params := route.Deserialize(mux.Vars(r))
		_, _ = fmt.Fprintf(w, "Namespace: %s, WorkflowID: %s\n", params.Namespace, params.WorkflowID)
	})
	recorder := httptest.NewRecorder()
	u := "http://localhost/" + route.Path(&MyRouteParams{
		Namespace:  "TEST-NAMESPACE",
		WorkflowID: "TEST-WORKFLOW-ID",
	})
	router.ServeHTTP(recorder, httptest.NewRequest("GET", u, nil))
	fmt.Println(recorder.Code)
	fmt.Println(recorder.Body.String())
	// Output:
	// 200
	// Namespace: TEST-NAMESPACE, WorkflowID: TEST-WORKFLOW-ID
}

func ExampleRoute_Representation() {
	route := newWorkflowRoute()
	fmt.Println(route.Representation())
	// Output: api/v1/namespaces/{namespace}/workflows/{workflowID}
}

func ExampleRoute_Path() {
	route := newWorkflowRoute()
	params := MyRouteParams{Namespace: "TEST-NAMESPACE", WorkflowID: "TEST-WORKFLOW-ID"}
	fmt.Println(route.Path(&params))
	// Output: api/v1/namespaces/TEST-NAMESPACE/workflows/TEST-WORKFLOW-ID
}

func ExampleRoute_Deserialize() {
	route := newWorkflowRoute()
	// Would usually be mux.Vars(r) in a real application
	vars := map[string]string{
		"namespace":  "TEST-NAMESPACE",
		"workflowID": "TEST-WORKFLOW-ID",
	}
	params := route.Deserialize(vars)
	fmt.Println(params.Namespace)
	fmt.Println(params.WorkflowID)
	// Output:
	// TEST-NAMESPACE
	// TEST-WORKFLOW-ID
}

func ExampleSlugs() {
	fmt.Println(routing.Slugs[MyRouteParams]("api", "v1", "namespaces").Representation())
	// Output: api/v1/namespaces
}

func ExampleStringParam() {
	fmt.Println(routing.StringParam("namespace", func(params *MyRouteParams) *string { return &params.Namespace }).Representation())
	// Output: {namespace}
}

func TestNewRoute(t *testing.T) {
	t.Parallel()

	route := newWorkflowRoute()

	t.Run("Path", func(t *testing.T) {
		assert.Equal(t, "api/v1/namespaces/{namespace}/workflows/{workflowID}", route.Representation())
	})

	t.Run("Serialize", func(t *testing.T) {
		params := MyRouteParams{Namespace: "TEST-NAMESPACE", WorkflowID: "TEST-WORKFLOW-ID"}
		assert.Equal(t, "api/v1/namespaces/TEST-NAMESPACE/workflows/TEST-WORKFLOW-ID", route.Path(&params))
	})

	t.Run("Deserialize", func(t *testing.T) {
		params := route.Deserialize(map[string]string{
			"namespace":  "TEST-NAMESPACE",
			"workflowID": "TEST-WORKFLOW-ID",
		})
		assert.Equal(t, &MyRouteParams{Namespace: "TEST-NAMESPACE", WorkflowID: "TEST-WORKFLOW-ID"}, params)
	})
}
