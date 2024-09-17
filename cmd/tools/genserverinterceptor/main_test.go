package main

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/api/matchingservice/v1"
)

func TestWorkflowTagGetters(t *testing.T) {
	requestT := reflect.TypeOf(&matchingservice.QueryWorkflowRequest{})
	md := workflowTagGetters(requestT, 0)
	assert.Equal(t, md.WorkflowIdGetter, "GetQueryRequest().GetExecution().GetWorkflowId()")
	assert.Equal(t, md.RunIdGetter, "GetQueryRequest().GetExecution().GetRunId()")
	assert.Equal(t, md.TaskTokenGetter, "")
}
