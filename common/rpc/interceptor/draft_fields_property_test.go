package interceptor

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	_ "go.temporal.io/api/operatorservice/v1"
	protometapb "go.temporal.io/api/protometa/v1"
	_ "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/draft"
	"go.temporal.io/server/common/draft/testfixture"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestDraftFieldsMatchDescriptors runs random requests and responses of every unary
// RPC from WorkflowService, OperatorService, and the fixture services with a
// random set of enabled features. The handler must receive, and the client must get,
// exactly the message with disabled draft contents cleared according to the descriptors.
// Disabled draft RPCs and requests that lose a draft enum value must be rejected.
func TestDraftFieldsMatchDescriptors(t *testing.T) {
	t.Parallel()
	methods := draftServiceMethods(t)
	features := apiDraftFeatures()
	var affected, unaffected []protoreflect.MethodDescriptor
	for _, method := range methods {
		if referenceMethodFeature(method) != "" || reachesDraft(method.Input(), nil) || reachesDraft(method.Output(), nil) {
			affected = append(affected, method)
		} else {
			unaffected = append(unaffected, method)
		}
	}
	if len(affected) == 0 {
		t.Fatal("no method reaches a draft field; the property would not exercise omission")
	}
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("namespace"), nil).AnyTimes()

	const iterations = 2000
	stripped := map[string]int{}
	rejected := map[codes.Code]int{}
	enumResponses := 0
	for seed := range uint64(iterations) {
		rng := rand.New(rand.NewPCG(seed, 0))
		candidates := unaffected
		if rng.IntN(2) == 0 || len(unaffected) == 0 {
			candidates = affected
		}
		method := candidates[rng.IntN(len(candidates))]
		var enabled []string
		for _, feature := range append(slices.Clone(features), "unknown-feature") {
			if rng.IntN(2) == 0 {
				enabled = append(enabled, feature)
			}
		}
		request := randomMessage(t, rng, method.Input())
		response := randomMessage(t, rng, method.Output())
		responseBefore := proto.Clone(response)

		i := NewDraftFieldsInterceptor(func(string) []string { return enabled }, registry, log.NewNoopLogger(), draft.Omit)
		fixtureServices := testfixture.File_common_draft_testfixture_filter_proto.Services()
		for j := 0; j < fixtureServices.Len(); j++ {
			i.addDraftService(fixtureServices.Get(j))
		}
		info := &grpc.UnaryServerInfo{FullMethod: fmt.Sprintf("/%s/%s", method.Parent().FullName(), method.Name())}
		var received proto.Message
		returned, err := i.OutgoingIntercept(t.Context(), proto.Clone(request), info, func(ctx context.Context, req any) (any, error) {
			return i.IncomingIntercept(ctx, req, info, func(_ context.Context, req any) (any, error) {
				received = proto.Clone(req.(proto.Message))
				return response, nil
			})
		})
		wantRequest := proto.Clone(request)
		removedEnum := stripDisabled(wantRequest.ProtoReflect(), enabled)
		var wantCode codes.Code
		switch feature := referenceMethodFeature(method); {
		case feature != "" && !slices.Contains(enabled, feature):
			wantCode = codes.Unimplemented
		case removedEnum:
			wantCode = codes.InvalidArgument
		default:
			wantCode = codes.OK
		}
		if status.Code(err) != wantCode {
			t.Fatalf("seed %d, %s: want %v with features %v, got %v", seed, info.FullMethod, wantCode, enabled, err)
		}
		if wantCode != codes.OK {
			rejected[wantCode]++
			require.Nil(t, received)
			if wantCode == codes.InvalidArgument {
				require.Equal(t, "request contains a disabled draft enum value", status.Convert(err).Message())
			}
			continue
		}
		require.True(t, proto.Equal(responseBefore, response), "seed %d mutated the response", seed)
		wantResponse := proto.Clone(responseBefore)
		if stripDisabled(wantResponse.ProtoReflect(), enabled) {
			enumResponses++
		}
		for _, check := range []struct {
			what                     string
			original, want, observed proto.Message
		}{{"request", request, wantRequest, received}, {"response", response, wantResponse, returned.(proto.Message)}} {
			if !proto.Equal(check.want, check.original) {
				stripped[check.what]++
			}
			if diff := cmp.Diff(check.want, check.observed, protocmp.Transform()); diff != "" {
				t.Fatalf("seed %d, %s %s with features %v differs from the descriptor reference (-want +got):\n%s",
					seed, info.FullMethod, check.what, enabled, diff)
			}
		}
	}
	require.Positive(t, rejected[codes.InvalidArgument])
	require.Positive(t, rejected[codes.Unimplemented])
	require.Positive(t, enumResponses)
	// Guard against a generator that never sets a draft field the features disable.
	for _, what := range []string{"request", "response"} {
		if stripped[what] == 0 {
			t.Fatalf("no %s had a draft field cleared", what)
		}
		t.Logf("%d of %d %ss had draft fields cleared", stripped[what], iterations, what)
	}
}

// draftServiceMethods returns the unary methods the draft interceptors apply to.
func draftServiceMethods(t *testing.T) []protoreflect.MethodDescriptor {
	t.Helper()
	var methods []protoreflect.MethodDescriptor
	for _, name := range []protoreflect.FullName{"temporal.api.workflowservice.v1.WorkflowService", "temporal.api.operatorservice.v1.OperatorService", "temporal.api.drafttest.v1.FixtureService", "temporal.api.drafttest.v1.DraftFixtureService"} {
		descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
		if err != nil {
			t.Fatal(err)
		}
		service := descriptor.(protoreflect.ServiceDescriptor)
		for i := 0; i < service.Methods().Len(); i++ {
			if method := service.Methods().Get(i); !method.IsStreamingClient() && !method.IsStreamingServer() {
				methods = append(methods, method)
			}
		}
	}
	return methods
}

// apiDraftFeatures returns every feature named by a draft annotation in the linked API.
func apiDraftFeatures() []string {
	var features []string
	var visit func(protoreflect.Descriptor)
	visit = func(d protoreflect.Descriptor) {
		if feature := referenceFeature(d); feature != "" {
			features = append(features, feature)
		}
		switch d := d.(type) {
		case protoreflect.FileDescriptor:
			for i := 0; i < d.Messages().Len(); i++ {
				visit(d.Messages().Get(i))
			}
			for i := 0; i < d.Enums().Len(); i++ {
				visit(d.Enums().Get(i))
			}
			for i := 0; i < d.Services().Len(); i++ {
				visit(d.Services().Get(i))
			}
		case protoreflect.MessageDescriptor:
			for i := 0; i < d.Messages().Len(); i++ {
				visit(d.Messages().Get(i))
			}
			for i := 0; i < d.Enums().Len(); i++ {
				visit(d.Enums().Get(i))
			}
			for i := 0; i < d.Fields().Len(); i++ {
				visit(d.Fields().Get(i))
			}
			for i := 0; i < d.Oneofs().Len(); i++ {
				visit(d.Oneofs().Get(i))
			}
		case protoreflect.EnumDescriptor:
			for i := 0; i < d.Values().Len(); i++ {
				visit(d.Values().Get(i))
			}
		case protoreflect.ServiceDescriptor:
			for i := 0; i < d.Methods().Len(); i++ {
				visit(d.Methods().Get(i))
			}
		default:
			// Fields, oneofs, enum values, and methods have no declarations to visit.
		}
	}
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if strings.HasPrefix(string(file.Package()), "temporal.api.") {
			visit(file)
		}
		return true
	})
	slices.Sort(features)
	return slices.Compact(features)
}

func referenceFeature(d protoreflect.Descriptor) string {
	var option protoreflect.ExtensionType
	switch d.(type) {
	case protoreflect.MessageDescriptor:
		option = protometapb.E_DraftMessage
	case protoreflect.FieldDescriptor:
		option = protometapb.E_Draft
	case protoreflect.OneofDescriptor:
		option = protometapb.E_DraftOneof
	case protoreflect.EnumDescriptor:
		option = protometapb.E_DraftEnum
	case protoreflect.EnumValueDescriptor:
		option = protometapb.E_DraftEnumValue
	case protoreflect.ServiceDescriptor:
		option = protometapb.E_DraftService
	case protoreflect.MethodDescriptor:
		option = protometapb.E_DraftMethod
	default:
		return ""
	}
	feature, _ := proto.GetExtension(d.Options(), option).(string)
	return feature
}

func referenceMethodFeature(method protoreflect.MethodDescriptor) string {
	if feature := referenceFeature(method); feature != "" {
		return feature
	}
	return referenceFeature(method.Parent())
}

// fieldDraftFeature returns the field's own feature or the feature of its oneof.
func fieldDraftFeature(field protoreflect.FieldDescriptor) string {
	if oneof := field.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() {
		if feature := referenceFeature(oneof); feature != "" {
			return feature
		}
	}
	return referenceFeature(field)
}

func hasDraftEnumValues(enum protoreflect.EnumDescriptor) bool {
	if enum == nil {
		return false
	}
	for i := 0; i < enum.Values().Len(); i++ {
		if referenceFeature(enum.Values().Get(i)) != "" {
			return true
		}
	}
	return false
}

// reachesDraft reports whether a message of this type can hold a draft field.
func reachesDraft(message protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) bool {
	if seen == nil {
		seen = map[protoreflect.FullName]bool{}
	}
	if referenceFeature(message) != "" {
		return true
	}
	if seen[message.FullName()] {
		return false
	}
	seen[message.FullName()] = true
	for i := 0; i < message.Fields().Len(); i++ {
		field := message.Fields().Get(i)
		if fieldDraftFeature(field) != "" {
			return true
		}
		if field.IsMap() {
			field = field.MapValue()
		}
		if hasDraftEnumValues(field.Enum()) || field.Message() != nil && reachesDraft(field.Message(), seen) {
			return true
		}
	}
	return false
}

// leadsToDraft reports whether field is draft or holds messages that can hold one.
func leadsToDraft(field protoreflect.FieldDescriptor) bool {
	if fieldDraftFeature(field) != "" {
		return true
	}
	if field.IsMap() {
		field = field.MapValue()
	}
	return hasDraftEnumValues(field.Enum()) || field.Message() != nil && reachesDraft(field.Message(), nil)
}

// stripDisabled clears disabled declarations and reports enum values removed from retained fields.
func stripDisabled(m protoreflect.Message, enabled []string) bool {
	if feature := referenceFeature(m.Descriptor()); feature != "" && !slices.Contains(enabled, feature) {
		proto.Reset(m.Interface())
		return false
	}
	removedEnum := false
	m.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if feature := fieldDraftFeature(field); feature != "" && !slices.Contains(enabled, feature) {
			m.Clear(field)
			return true
		}
		stripValue := func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if field.Message() != nil {
				removedEnum = stripDisabled(value.Message(), enabled) || removedEnum
			}
			if field.Enum() != nil {
				if enumValue := field.Enum().Values().ByNumber(value.Enum()); enumValue != nil {
					feature := referenceFeature(enumValue)
					if feature != "" && !slices.Contains(enabled, feature) {
						removedEnum = true
						return true
					}
				}
			}
			return false
		}
		switch {
		case field.IsList():
			list := value.List()
			kept := 0
			for i := 0; i < list.Len(); i++ {
				if entry := list.Get(i); !stripValue(field, entry) {
					list.Set(kept, entry)
					kept++
				}
			}
			list.Truncate(kept)
		case field.IsMap():
			value.Map().Range(func(key protoreflect.MapKey, entry protoreflect.Value) bool {
				if stripValue(field.MapValue(), entry) {
					value.Map().Clear(key)
				}
				return true
			})
		default:
			if stripValue(field, value) {
				m.Clear(field)
			}
		}
		return true
	})
	return removedEnum
}

func randomMessage(t *testing.T, rng *rand.Rand, descriptor protoreflect.MessageDescriptor) proto.Message {
	t.Helper()
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(descriptor.FullName())
	if err != nil {
		t.Fatal(err)
	}
	message := messageType.New()
	fillRandom(rng, message, 0)
	return message.Interface()
}

// fillRandom sets a random selection of m's fields. Fields that are draft or lead to one
// are always set, first so that they win their oneof, and may nest up to eight messages
// deep, enough to reach drafts inside history events; other messages stop at three.
func fillRandom(rng *rand.Rand, m protoreflect.Message, depth int) {
	fields := m.Descriptor().Fields()
	for _, draftPass := range []bool{true, false} {
		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)
			leads := leadsToDraft(field)
			if leads != draftPass {
				continue
			}
			if oneof := field.ContainingOneof(); oneof != nil && m.WhichOneof(oneof) != nil {
				continue
			}
			if field.Message() != nil && (depth >= 8 || (!leads && depth >= 3)) {
				continue
			}
			if !leads && rng.IntN(2) == 0 {
				continue
			}
			switch {
			case field.IsList():
				list := m.Mutable(field).List()
				for range 1 + rng.IntN(2) {
					list.Append(randomValue(rng, list.NewElement, field, depth))
				}
			case field.IsMap():
				entries := m.Mutable(field).Map()
				for range 1 + rng.IntN(2) {
					key := randomValue(rng, nil, field.MapKey(), depth).MapKey()
					entries.Set(key, randomValue(rng, entries.NewValue, field.MapValue(), depth))
				}
			default:
				m.Set(field, randomValue(rng, func() protoreflect.Value { return m.NewField(field) }, field, depth))
			}
		}
	}
}

func randomValue(rng *rand.Rand, newMessage func() protoreflect.Value, field protoreflect.FieldDescriptor, depth int) protoreflect.Value {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(rng.IntN(2) == 0)
	case protoreflect.EnumKind:
		if rng.IntN(5) == 0 {
			return protoreflect.ValueOfEnum(-12345)
		}
		values := field.Enum().Values()
		return protoreflect.ValueOfEnum(values.Get(rng.IntN(values.Len())).Number())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(rng.Int32())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(rng.Int64())
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(rng.Uint32())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(rng.Uint64())
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(float32(rng.IntN(1000)))
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(float64(rng.IntN(1000)))
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(fmt.Sprintf("s%d", rng.IntN(1000)))
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{byte(rng.IntN(256))})
	case protoreflect.MessageKind, protoreflect.GroupKind:
		message := newMessage()
		fillRandom(rng, message.Message(), depth+1)
		return message
	default:
		panic(fmt.Sprintf("unhandled kind %v", field.Kind()))
	}
}
