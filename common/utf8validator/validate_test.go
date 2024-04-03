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

package utf8validator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/operatorservice/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestValidate(t *testing.T) {
	invalid := "some \x80 error"

	cases := []struct {
		name    string
		message proto.Message
		badPath string
	}{
		{
			name: "nothing invalid",
			message: &workflowservice.StartWorkflowExecutionRequest{
				Namespace:    "regular text here",
				WorkflowId:   "nothing invalid",
				WorkflowType: &commonpb.WorkflowType{Name: "all ascii"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "or maybe some unicode \u2603"},
				Identity:     "even\x00control\x01characters\x02are\x03fine",
			},
			badPath: "",
		},
		{
			name:    "bytes do not have to be valid utf-8",
			message: &commonpb.Payload{Data: []byte("'3\xf7).\xf8 n\x92\x08j\x1c(1x\x0e")},
			badPath: "",
		},
		{
			name: "simple string field",
			message: &workflowservice.StartWorkflowExecutionRequest{
				Namespace: "uh oh \xfe",
			},
			badPath: "namespace",
		},
		{
			name: "nested string field",
			message: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowType: &commonpb.WorkflowType{Name: invalid},
			},
			badPath: "workflow_type.name",
		},
		{
			name: "repeated string field",
			message: &operatorservice.RemoveSearchAttributesRequest{
				SearchAttributes: []string{"zero is okay", "one is fine too", "two is bad:" + invalid, "three's good"},
			},
			badPath: "search_attributes.[2]",
		},
		{
			name: "string map value",
			message: &namespacepb.NamespaceInfo{
				Data: map[string]string{"goodfield": invalid},
			},
			badPath: `data.["goodfield"].value`,
		},
		{
			name: "string map key",
			message: &namespacepb.NamespaceInfo{
				Data: map[string]string{"bad\x80field": "value is ok"},
			},
			badPath: `data.["bad\x80field"].key`,
		},
		{
			name: "string inside oneof",
			message: &commonpb.ResetOptions{
				Target: &commonpb.ResetOptions_BuildId{
					BuildId: invalid,
				},
			},
			badPath: "build_id",
		},
		{
			name: "string inside repeated message",
			message: &replicationpb.NamespaceReplicationConfig{
				Clusters: []*replicationpb.ClusterReplicationConfig{
					{ClusterName: "good"},
					{ClusterName: "oops" + invalid},
				},
			},
			badPath: "clusters.[1].cluster_name",
		},
		{
			name: "string map key inside repeated message inside map value",
			message: &commandpb.RecordMarkerCommandAttributes{
				Details: map[string]*commonpb.Payloads{
					"detail": &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							&commonpb.Payload{
								Metadata: map[string][]byte{
									"ok key:":    []byte("binary\x80data\xddis\xeeokay"),
									"\xbbad key": nil,
								},
							},
						},
					},
				},
			},
			badPath: `details.["detail"].value.payloads.[0].metadata.["\xbbad key"].key`,
		},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var validation validation
			validation.validateMessage(c.message.ProtoReflect())
			assert.Equal(t, c.badPath, validation.badPath, i)
		})
	}
}

func TestSample(t *testing.T) {
	v := newValidator(
		log.NewTestLogger(),
		metricstest.NewCaptureHandler(),
		dynamicconfig.GetFloatPropertyFn(0.5),
		dynamicconfig.GetFloatPropertyFn(0.0),
		dynamicconfig.GetFloatPropertyFn(1.0),
		dynamicconfig.GetBoolPropertyFn(true),
		dynamicconfig.GetBoolPropertyFn(true),
		dynamicconfig.GetBoolPropertyFn(true),
	)

	msg := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: "uh oh \xfe",
	}

	var requestErrors, responseErrors, persistenceErrors int
	for i := 0; i < 1000; i++ {
		if v.Validate(msg, SourceRPCRequest) != nil {
			requestErrors++
		}
		if v.Validate(msg, SourceRPCResponse) != nil {
			responseErrors++
		}
		if v.Validate(msg, SourcePersistence) != nil {
			persistenceErrors++
		}
	}
	assert.Greater(t, requestErrors, 400)
	assert.Less(t, requestErrors, 600)
	assert.Equal(t, responseErrors, 0)
	assert.Equal(t, persistenceErrors, 1000)
}

func testInterceptGeneric(t *testing.T, shouldFail bool) {
	captureHandler := metricstest.NewCaptureHandler()

	v := newValidator(
		log.NewTestLogger(),
		captureHandler,
		dynamicconfig.GetFloatPropertyFn(1.0),
		dynamicconfig.GetFloatPropertyFn(1.0),
		dynamicconfig.GetFloatPropertyFn(1.0),
		dynamicconfig.GetBoolPropertyFn(shouldFail),
		dynamicconfig.GetBoolPropertyFn(shouldFail),
		dynamicconfig.GetBoolPropertyFn(shouldFail),
	)

	// request is invalid
	capture := captureHandler.StartCapture()
	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace: "uh oh \xfe",
	}
	_, err := v.Intercept(
		context.Background(),
		req,
		&grpc.UnaryServerInfo{FullMethod: "start workflow"},
		func(ctx context.Context, req any) (any, error) {
			return nil, nil
		})
	if shouldFail {
		var invalidArg *serviceerror.InvalidArgument
		assert.ErrorAs(t, err, &invalidArg)
		assert.ErrorContains(t, err, ErrInvalidUTF8.Error())
	} else {
		assert.NoError(t, err)
	}
	assert.NotEmpty(t, capture.Snapshot()[metrics.UTF8ValidationErrors.Name()])
	captureHandler.StopCapture(capture)

	// response is invalid
	capture = captureHandler.StartCapture()
	req = &workflowservice.StartWorkflowExecutionRequest{
		Namespace: "ok",
	}
	_, err = v.Intercept(
		context.Background(),
		req,
		&grpc.UnaryServerInfo{FullMethod: "start workflow"},
		func(ctx context.Context, req any) (any, error) {
			return &workflowservice.StartWorkflowExecutionResponse{
				RunId: "oops \xee",
			}, nil
		})
	if shouldFail {
		var internal *serviceerror.Internal
		assert.ErrorAs(t, err, &internal)
		assert.ErrorContains(t, err, ErrInvalidUTF8.Error())
	} else {
		assert.NoError(t, err)
	}
	assert.NotEmpty(t, capture.Snapshot()[metrics.UTF8ValidationErrors.Name()])
	captureHandler.StopCapture(capture)
}

func TestInterceptWithoutFail(t *testing.T) {
	testInterceptGeneric(t, false)
}
func TestInterceptWithFail(t *testing.T) {
	testInterceptGeneric(t, true)
}
