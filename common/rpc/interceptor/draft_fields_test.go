package interceptor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	errordetailspb "go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/draft"
	"go.temporal.io/server/common/draft/testfixture"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/emptypb"
)

const draftTestMethod = "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"

type draftNamedRequest struct {
	*testfixture.Message
	name string
}

func (r *draftNamedRequest) GetNamespace() string { return r.name }

type draftUnaffectedRequest struct {
	*emptypb.Empty
	name string
}

func (r *draftUnaffectedRequest) GetNamespace() string { return r.name }

func TestDraftFieldsSharedResponse(t *testing.T) {
	i := NewDraftFieldsInterceptor(func(string) []string { return []string{"feature-a"} }, nil, log.NewNoopLogger(), draft.Omit)
	shared := &testfixture.Parent{Message: draftMessage()}
	out, err := draftInvoke(i, &emptypb.Empty{}, nil, func(context.Context, any) (any, error) {
		return shared, nil
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &testfixture.Parent{Message: &testfixture.Message{Stable: "stable", FeatureA: "a"}}, out.(*testfixture.Parent))
	protorequire.ProtoEqual(t, &testfixture.Parent{Message: draftMessage()}, shared)
}

func TestDraftFieldsErrorDetails(t *testing.T) {
	stable := &errordetailspb.NamespaceNotFoundFailure{Namespace: "missing"}
	detail := &testfixture.Message{Stable: "stable", FeatureA: "a"}
	st, err := status.New(codes.NotFound, "namespace is missing").WithDetails(stable, detail)
	require.NoError(t, err)
	var original *serviceerror.NamespaceNotFound
	require.ErrorAs(t, serviceerror.FromStatus(st), &original)
	originalStatus := st.Proto()

	for _, tc := range []struct {
		name    string
		enabled []string
		want    *testfixture.Message
	}{
		{"disabled", nil, &testfixture.Message{Stable: "stable"}},
		{"enabled", []string{"feature-a"}, &testfixture.Message{Stable: "stable", FeatureA: "a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i := NewDraftFieldsInterceptor(func(string) []string { return tc.enabled }, nil, log.NewNoopLogger(), draft.Omit)
			out, err := draftInvoke(i, &emptypb.Empty{}, nil, func(context.Context, any) (any, error) {
				return nil, original
			})
			require.Nil(t, out)
			require.Error(t, err)
			protorequire.ProtoEqual(t, originalStatus, original.Status().Proto())
			var got *status.Status
			if len(tc.enabled) == 0 {
				var ok bool
				got, ok = status.FromError(err)
				require.True(t, ok, "filtered error must carry a gRPC status")
			} else {
				require.Same(t, original, err)
				got = original.Status()
			}
			require.Equal(t, codes.Code(originalStatus.GetCode()), got.Code())
			require.Equal(t, originalStatus.GetMessage(), got.Message())
			details := got.Details()
			require.Len(t, details, 2)
			require.IsType(t, stable, details[0])
			require.IsType(t, tc.want, details[1])
			protorequire.ProtoEqual(t, stable, details[0].(*errordetailspb.NamespaceNotFoundFailure))
			protorequire.ProtoEqual(t, tc.want, details[1].(*testfixture.Message))
		})
	}
}

func TestDraftFieldsUnaffectedErrorDetails(t *testing.T) {
	i := NewDraftFieldsInterceptor(func(string) []string { t.Fatal("unexpected config lookup"); return nil }, nil, log.NewNoopLogger(), draft.Omit)
	original := serviceerror.NewNamespaceNotFound("missing")
	_, err := draftInvoke(i, &emptypb.Empty{}, nil, func(context.Context, any) (any, error) {
		return nil, original
	})
	require.Same(t, original, err)
}

func TestDraftFieldsErrorDetailOmissionFailure(t *testing.T) {
	st, err := status.New(codes.NotFound, "namespace is missing").WithDetails(draftMessage())
	require.NoError(t, err)
	i := NewDraftFieldsInterceptor(func(string) []string { return nil }, nil, log.NewNoopLogger(), func(msg any, _ *draft.Selection) (bool, error) {
		require.IsType(t, &testfixture.Message{}, msg)
		return true, errors.New("sensitive payload")
	})
	out, err := draftInvoke(i, nil, nil, func(context.Context, any) (any, error) {
		return nil, st.Err()
	})
	require.Nil(t, out)
	require.Error(t, err)
	got, ok := status.FromError(err)
	require.True(t, ok)
	protorequire.ProtoEqual(t, status.New(codes.Internal, "draft field omission failed").Proto(), got.Proto())
}

func TestDraftFieldsServicesAreUnary(t *testing.T) {
	for _, name := range []protoreflect.FullName{
		"temporal.api.workflowservice.v1.WorkflowService",
		"temporal.api.operatorservice.v1.OperatorService",
	} {
		descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
		require.NoError(t, err)
		methods := descriptor.(protoreflect.ServiceDescriptor).Methods()
		for idx := range methods.Len() {
			method := methods.Get(idx)
			require.False(t, method.IsStreamingClient() || method.IsStreamingServer(),
				"%s is streaming. The draft filter only covers unary calls. A stream interceptor must be added first.", method.FullName())
		}
	}
}

func TestDraftFieldsResponseOnlyNamespaceBinding(t *testing.T) {
	reads := 0
	i := NewDraftFieldsInterceptor(func(name string) []string {
		reads++
		require.Equal(t, "disabled", name)
		return nil
	}, nil, log.NewNoopLogger(), draft.Omit)
	req := &draftUnaffectedRequest{Empty: &emptypb.Empty{}, name: "disabled"}
	response := draftMessage()
	out, err := draftInvoke(i, req, nil, func(context.Context, any) (any, error) {
		require.Zero(t, reads)
		req.name = "enabled"
		return response, nil
	})
	require.NoError(t, err)
	require.Empty(t, out.(*testfixture.Message).GetFeatureA())
	require.Equal(t, 1, reads)
}

func draftMessage() *testfixture.Message {
	return &testfixture.Message{Stable: "stable", FeatureA: "a", FeatureB: "b"}
}

func draftInvoke(i *DraftFieldsInterceptor, req any, middle grpc.UnaryServerInterceptor, handler grpc.UnaryHandler) (any, error) {
	info := &grpc.UnaryServerInfo{FullMethod: draftTestMethod}
	return i.OutgoingIntercept(context.Background(), req, info, func(ctx context.Context, req any) (any, error) {
		next := func(ctx context.Context, req any) (any, error) { return i.IncomingIntercept(ctx, req, info, handler) }
		if middle != nil {
			return middle(ctx, req, info, next)
		}
		return next(ctx, req)
	})
}

func TestDraftFieldsSelections(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled []string
		a, b    string
	}{
		{"empty", nil, "", ""}, {"a", []string{"feature-a"}, "a", ""},
		{"b", []string{"feature-b"}, "", "b"}, {"both", []string{"feature-a", "feature-b"}, "a", "b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads := 0
			configured := tc.enabled
			i := NewDraftFieldsInterceptor(func(name string) []string { reads++; require.Equal(t, "tenant", name); return configured }, nil, log.NewNoopLogger(), draft.Omit)
			req := &draftNamedRequest{Message: draftMessage(), name: "tenant"}
			response := draftMessage()
			out, err := draftInvoke(i, req, nil, func(_ context.Context, got any) (any, error) {
				require.Same(t, req, got)
				require.Equal(t, tc.a, req.FeatureA)
				require.Equal(t, tc.b, req.FeatureB)
				configured = []string{"feature-a", "feature-b"}
				return response, nil
			})
			require.NoError(t, err)
			if tc.a != "" && tc.b != "" {
				require.Same(t, response, out)
			}
			protorequire.ProtoEqual(t, &testfixture.Message{Stable: "stable", FeatureA: tc.a, FeatureB: tc.b}, out.(*testfixture.Message))
			require.Equal(t, "stable", req.Stable)
			require.Equal(t, 1, reads)
		})
	}
}

func TestDraftFieldsNamespacePolicy(t *testing.T) {
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceName(namespace.ID("known")).Return(namespace.Name("enabled"), nil)
	registry.EXPECT().GetNamespaceName(namespace.ID("missing")).Return(namespace.EmptyName, serviceerror.NewNamespaceNotFound("missing"))
	registry.EXPECT().GetNamespaceName(namespace.ID("describe-known")).Return(namespace.Name("disabled"), nil)
	registry.EXPECT().GetNamespaceName(namespace.ID("describe-missing")).Return(namespace.EmptyName, serviceerror.NewNamespaceNotFound("describe-missing"))
	for _, tc := range []struct {
		name       string
		req        any
		want       string
		configName string
		reads      int
	}{
		{"enabled", &draftNamedRequest{Message: draftMessage(), name: "enabled"}, "a", "enabled", 1},
		{"disabled", &draftNamedRequest{Message: draftMessage(), name: "disabled"}, "", "disabled", 1},
		{"global", &workflowservice.GetSystemInfoRequest{}, "a", "", 1},
		{"known-id", &operatorservice.DeleteNamespaceRequest{NamespaceId: "known"}, "a", "enabled", 1},
		{"unknown-id", &operatorservice.DeleteNamespaceRequest{NamespaceId: "missing"}, "", "", 0},
		{"describe-id", &workflowservice.DescribeNamespaceRequest{Id: "describe-known"}, "", "disabled", 1},
		{"describe-unknown-id", &workflowservice.DescribeNamespaceRequest{Id: "describe-missing"}, "", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads := 0
			i := NewDraftFieldsInterceptor(func(name string) []string {
				reads++
				require.Equal(t, tc.configName, name)
				if name == "disabled" {
					return nil
				}
				return []string{"feature-a"}
			}, registry, log.NewNoopLogger(), draft.Omit)
			response := draftMessage()
			out, err := draftInvoke(i, tc.req, nil, func(context.Context, any) (any, error) { return response, nil })
			require.NoError(t, err)
			require.Equal(t, tc.want, out.(*testfixture.Message).GetFeatureA())
			require.Equal(t, tc.reads, reads)
		})
	}
}

func TestDraftFieldsNamespaceNormalization(t *testing.T) {
	for _, query := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			registry := namespace.NewMockRegistry(gomock.NewController(t))
			registry.EXPECT().GetNamespaceByID(namespace.ID("id")).Return(namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: "normalized"}, nil, "local"), nil)
			serializer := tasktoken.NewSerializer()
			var req any
			if query {
				token, err := serializer.SerializeQueryTaskToken(&tokenspb.QueryTask{NamespaceId: "id"})
				require.NoError(t, err)
				req = &workflowservice.RespondQueryTaskCompletedRequest{TaskToken: token}
			} else {
				token, err := serializer.Serialize(&tokenspb.Task{NamespaceId: "id"})
				require.NoError(t, err)
				req = &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: token}
			}
			reads := 0
			i := NewDraftFieldsInterceptor(func(name string) []string {
				reads++
				require.Equal(t, "normalized", name)
				if enabled {
					return []string{"feature-a"}
				}
				return nil
			}, registry, log.NewNoopLogger(), draft.Omit)
			validator := NewNamespaceValidatorInterceptor(registry, dynamicconfig.GetBoolPropertyFn(false), dynamicconfig.GetIntPropertyFn(100), nil)
			response := draftMessage()
			out, err := draftInvoke(i, req, validator.NamespaceValidateIntercept, func(context.Context, any) (any, error) { return response, nil })
			require.NoError(t, err)
			require.Equal(t, 1, reads)
			if enabled {
				require.Equal(t, "a", out.(*testfixture.Message).GetFeatureA())
			} else {
				require.Empty(t, out.(*testfixture.Message).GetFeatureA())
			}
		}
	}
}

func TestDraftFieldsFailuresAndPassthrough(t *testing.T) {
	i := NewDraftFieldsInterceptor(func(string) []string { t.Fatal("unexpected config lookup"); return nil }, nil, log.NewNoopLogger(), draft.Omit)
	validator := NewNamespaceValidatorInterceptor(nil, dynamicconfig.GetBoolPropertyFn(false), dynamicconfig.GetIntPropertyFn(100), nil)
	_, err := draftInvoke(i, &workflowservice.RespondActivityTaskCompletedRequest{TaskToken: []byte{255}}, validator.NamespaceValidateIntercept, func(context.Context, any) (any, error) { t.Fatal("handler ran"); return nil, nil })
	require.Error(t, err)
	for _, method := range []string{draftTestMethod, "/temporal.server.api.adminservice.v1.AdminService/Test"} {
		response := &emptypb.Empty{}
		out, err := i.OutgoingIntercept(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { return response, nil })
		require.NoError(t, err)
		require.Same(t, response, out)
	}
	original := errors.New("handler error")
	out, err := draftInvoke(i, nil, nil, func(context.Context, any) (any, error) { return draftMessage(), original })
	require.Same(t, original, err)
	require.NotNil(t, out)
	require.PanicsWithValue(t, "handler panic", func() {
		_, _ = draftInvoke(i, nil, nil, func(context.Context, any) (any, error) { panic("handler panic") })
	})
	i.omit = func(any, *draft.Selection) (bool, error) { return true, errors.New("sensitive payload") }
	out, err = draftInvoke(i, draftMessage(), nil, func(context.Context, any) (any, error) { t.Fatal("handler ran"); return nil, nil })
	require.Nil(t, out)
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), "sensitive")
}

func TestDraftFieldsOmissionBeforeValidation(t *testing.T) {
	i := NewDraftFieldsInterceptor(func(string) []string { return nil }, nil, log.NewNoopLogger(), draft.Omit)
	_, err := draftInvoke(i, draftMessage(), nil, func(_ context.Context, req any) (any, error) {
		if req.(*testfixture.Message).FeatureA == "" {
			return nil, status.Error(codes.InvalidArgument, "required field missing")
		}
		t.Fatal("disabled field reached validation")
		return nil, nil
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestDraftFieldsConfigPanic(t *testing.T) {
	for _, incoming := range []bool{false, true} {
		core, logs := observer.New(zap.ErrorLevel)
		i := NewDraftFieldsInterceptor(func(string) []string { panic("sensitive config") }, nil, log.NewZapLogger(zap.New(core)), draft.Omit)
		var req any = &emptypb.Empty{}
		if incoming {
			req = draftMessage()
		}
		called := false
		out, err := draftInvoke(i, req, nil, func(context.Context, any) (any, error) {
			called = true
			return draftMessage(), nil
		})
		require.Equal(t, !incoming, called)
		require.Nil(t, out)
		require.Equal(t, status.New(codes.Internal, "draft field omission failed"), status.Convert(err))
		entries := logs.All()
		require.Len(t, entries, 1)
		fields := entries[0].ContextMap()
		require.Equal(t, draftTestMethod, fields["operation"])
		require.Contains(t, fields["error"], "temporal.api.drafttest.v1.Message")
		require.Contains(t, fields["error"], "sensitive config")
	}
}

func TestDraftFieldsExcludedAndNonProto(t *testing.T) {
	i := NewDraftFieldsInterceptor(func(string) []string { t.Fatal("unexpected config lookup"); return nil }, nil, log.NewNoopLogger(), draft.Omit)
	for _, method := range []string{"/temporal.server.api.adminservice.v1.AdminService/Test", "/temporal.api.workflowservice.v1.WorkflowServiceExtra/Test"} {
		root := draftMessage()
		out, err := i.OutgoingIntercept(t.Context(), root, &grpc.UnaryServerInfo{FullMethod: method}, func(ctx context.Context, req any) (any, error) {
			return i.IncomingIntercept(ctx, req, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { return root, nil })
		})
		require.NoError(t, err)
		require.Same(t, root, out)
		require.Equal(t, "a", root.FeatureA)
	}
	out, err := draftInvoke(i, "request", nil, func(context.Context, any) (any, error) { return "response", nil })
	require.NoError(t, err)
	require.Equal(t, "response", out)
}
