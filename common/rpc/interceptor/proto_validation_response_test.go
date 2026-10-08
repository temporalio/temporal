package interceptor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const nexusStartMethod = "/temporal.api.workflowservice.v1.WorkflowService/StartNexusOperationExecution"

func validNexusStartRequest() *workflowservice.StartNexusOperationExecutionRequest {
	return &workflowservice.StartNexusOperationExecutionRequest{
		Namespace: "ns", OperationId: "id", Endpoint: "endpoint", Service: "service", Operation: "operation",
	}
}

type responseFailureValidator struct {
	protovalidate.Validator
	failure    error
	panicValue any
}

func (v responseFailureValidator) Validate(message proto.Message, options ...protovalidate.ValidationOption) error {
	if _, ok := message.(*workflowservice.StartNexusOperationExecutionResponse); ok {
		if v.panicValue != nil {
			panic(v.panicValue)
		}
		return v.failure
	}
	return v.Validator.Validate(message, options...)
}

func TestProtoValidationResponseFailuresPreserveSuccessfulResult(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"implementation error", "panic"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			core, entries := observer.New(zap.ErrorLevel)
			interceptor, err := NewProtoValidationInterceptor(log.NewZapLogger(zap.New(core)), metrics.NoopMetricsHandler)
			require.NoError(t, err)
			validator := responseFailureValidator{Validator: interceptor.validator, failure: errors.New("private response value")}
			if scenario == "panic" {
				validator.panicValue = "private response value"
			}
			interceptor.validator = validator
			response := &workflowservice.StartNexusOperationExecutionResponse{RunId: "run"}
			var result any
			require.NotPanics(t, func() {
				result, err = interceptor.Intercept(t.Context(), validNexusStartRequest(), &grpc.UnaryServerInfo{FullMethod: nexusStartMethod},
					func(context.Context, any) (any, error) { return response, nil })
			})
			require.NoError(t, err)
			require.Same(t, response, result)
			require.Len(t, entries.All(), 1)
			require.Equal(t, true, entries.All()[0].ContextMap()["failed-assertion"])
			require.Equal(t, "implementation", entries.All()[0].ContextMap()["failure_type"])
			require.NotContains(t, entries.All()[0].ContextMap(), "error")
		})
	}
}

func TestProtoValidationResponseReportsAreThrottled(t *testing.T) {
	t.Parallel()
	core, entries := observer.New(zap.ErrorLevel)
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	interceptor, err := NewProtoValidationInterceptor(log.NewZapLogger(zap.New(core)), metricsHandler)
	require.NoError(t, err)
	response := &workflowservice.StartNexusOperationExecutionResponse{}
	for range 100 {
		result, err := interceptor.Intercept(t.Context(), validNexusStartRequest(), &grpc.UnaryServerInfo{FullMethod: nexusStartMethod},
			func(context.Context, any) (any, error) { return response, nil })
		require.NoError(t, err)
		require.Same(t, response, result)
	}
	require.Positive(t, entries.Len())
	require.Less(t, entries.Len(), 100)
	recordings := capture.SnapshotMetric("response_validation_failures")
	require.Len(t, recordings, 100)
	require.Equal(t, map[string]string{"operation": nexusStartMethod, "validation_failure_type": "violation"}, recordings[0].Tags)
}

func TestProtoValidationPreservesHandlerErrors(t *testing.T) {
	t.Parallel()
	core, entries := observer.New(zap.ErrorLevel)
	interceptor, err := NewProtoValidationInterceptor(log.NewZapLogger(zap.New(core)), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	response := &workflowservice.StartNexusOperationExecutionResponse{}
	handlerError := errors.New("handler error")
	result, err := interceptor.Intercept(t.Context(), validNexusStartRequest(), &grpc.UnaryServerInfo{FullMethod: nexusStartMethod},
		func(context.Context, any) (any, error) { return response, handlerError })
	require.ErrorIs(t, err, handlerError)
	require.Same(t, response, result)
	require.Empty(t, entries.All())
}

func TestProtoValidationResponseDiagnosticsAreBoundedAndRedacted(t *testing.T) {
	t.Parallel()
	core, entries := observer.New(zap.ErrorLevel)
	interceptor, err := NewProtoValidationInterceptor(log.NewZapLogger(zap.New(core)), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	interceptor.validator = responseFailureValidator{Validator: interceptor.validator, failure: nestedResponseValidationFailure(t)}
	response := &workflowservice.StartNexusOperationExecutionResponse{RunId: "run"}
	result, err := interceptor.Intercept(t.Context(), validNexusStartRequest(), &grpc.UnaryServerInfo{FullMethod: nexusStartMethod},
		func(context.Context, any) (any, error) { return response, nil })
	require.NoError(t, err)
	require.Same(t, response, result)
	require.Len(t, entries.All(), 1)
	fields := entries.All()[0].ContextMap()
	require.Equal(t, int64(20), fields["violation_count"])
	require.Equal(t, "violation", fields["failure_type"])
	require.NotContains(t, fields, "error")
	paths, ok := fields["field_paths"].([]interface{})
	require.True(t, ok)
	require.Len(t, paths, 10)
	for _, path := range paths {
		require.Equal(t, "values[*].name", path)
	}
}

func nestedResponseValidationFailure(t *testing.T) error {
	t.Helper()
	options := new(descriptorpb.FieldOptions)
	proto.SetExtension(options, validate.E_Field, (&validate.FieldRules_builder{Required: new(true)}).Build())
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("response_diagnostics_test.proto"), Package: new("interceptor.response"), Syntax: new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: new("Child"), Field: []*descriptorpb.FieldDescriptorProto{{Name: new("name"), Number: new(int32(1)), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: options}}},
			{Name: new("Root"), NestedType: []*descriptorpb.DescriptorProto{{Name: new("ValuesEntry"), Options: &descriptorpb.MessageOptions{MapEntry: new(true)}, Field: []*descriptorpb.FieldDescriptorProto{
				{Name: new("key"), Number: new(int32(1)), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
				{Name: new("value"), Number: new(int32(2)), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: new(".interceptor.response.Child")},
			}}}, Field: []*descriptorpb.FieldDescriptorProto{{Name: new("values"), Number: new(int32(1)), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: new(".interceptor.response.Root.ValuesEntry")}}},
		},
	}, nil)
	require.NoError(t, err)
	root := dynamicpb.NewMessage(file.Messages().ByName("Root"))
	field := root.Descriptor().Fields().ByName("values")
	values := root.Mutable(field).Map()
	for index := range 20 {
		values.Set(protoreflect.ValueOfString(fmt.Sprintf("private-\\\"]key-%d", index)).MapKey(), protoreflect.ValueOfMessage(dynamicpb.NewMessage(field.MapValue().Message())))
	}
	validator, err := protovalidate.New(protovalidate.WithMessages(root))
	require.NoError(t, err)
	err = validator.Validate(root)
	require.Error(t, err)
	return err
}

func TestProtoValidationUnannotatedRPCsPassThrough(t *testing.T) {
	t.Parallel()
	core, entries := observer.New(zap.ErrorLevel)
	interceptor, err := NewProtoValidationInterceptor(log.NewZapLogger(zap.New(core)), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	response := &workflowservice.GetSystemInfoResponse{}
	result, err := interceptor.Intercept(t.Context(), &workflowservice.GetSystemInfoRequest{},
		&grpc.UnaryServerInfo{FullMethod: "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"},
		func(context.Context, any) (any, error) { return response, nil })
	require.NoError(t, err)
	require.Same(t, response, result)
	require.Empty(t, entries.All())
}
