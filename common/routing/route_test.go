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

type QualifiedWorkflow struct {
	Namespace  string
	WorkflowID string
}

func newWorkflowRoute() routing.Route[QualifiedWorkflow] {
	return routing.NewBuilder[QualifiedWorkflow]().
		Constant("api", "v1", "namespaces").
		StringVariable("namespace", func(params *QualifiedWorkflow) *string { return &params.Namespace }).
		Constant("workflows").
		StringVariable("workflowID", func(params *QualifiedWorkflow) *string { return &params.WorkflowID }).
		Build()
}

func ExampleRoute() {
	route := routing.NewBuilder[QualifiedWorkflow]().
		Constant("api", "v1", "namespaces").
		StringVariable("namespace", func(params *QualifiedWorkflow) *string { return &params.Namespace }).
		Constant("workflows").
		StringVariable("workflowID", func(params *QualifiedWorkflow) *string { return &params.WorkflowID }).
		Build()
	router := mux.NewRouter()
	router.HandleFunc("/"+route.Representation(), func(w http.ResponseWriter, r *http.Request) {
		params := route.Deserialize(mux.Vars(r))
		_, _ = fmt.Fprintf(w, "Namespace: %s, WorkflowID: %s\n", params.Namespace, params.WorkflowID)
	})
	recorder := httptest.NewRecorder()
	u := "http://localhost/" + route.Path(QualifiedWorkflow{
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
	params := QualifiedWorkflow{Namespace: "TEST-NAMESPACE", WorkflowID: "TEST-WORKFLOW-ID"}
	fmt.Println(route.Path(params))
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

func ExampleConstant() {
	fmt.Println(routing.Constant[QualifiedWorkflow]("api", "v1", "namespaces").Representation())
	// Output: api/v1/namespaces
}

func ExampleStringVariable() {
	fmt.Println(routing.StringVariable("namespace", func(params *QualifiedWorkflow) *string { return &params.Namespace }).Representation())
	// Output: {namespace}
}

func TestNewRoute(t *testing.T) {
	t.Parallel()

	route := newWorkflowRoute()

	t.Run("Path", func(t *testing.T) {
		assert.Equal(t, "api/v1/namespaces/{namespace}/workflows/{workflowID}", route.Representation())
	})

	t.Run("Get", func(t *testing.T) {
		params := QualifiedWorkflow{Namespace: "TEST-NAMESPACE", WorkflowID: "TEST-WORKFLOW-ID"}
		assert.Equal(t, "api/v1/namespaces/TEST-NAMESPACE/workflows/TEST-WORKFLOW-ID", route.Path(params))
	})

	t.Run("Set", func(t *testing.T) {
		params := route.Deserialize(map[string]string{
			"namespace":  "TEST-NAMESPACE",
			"workflowID": "TEST-WORKFLOW-ID",
		})
		assert.Equal(t, QualifiedWorkflow{Namespace: "TEST-NAMESPACE", WorkflowID: "TEST-WORKFLOW-ID"}, params)
	})
}
