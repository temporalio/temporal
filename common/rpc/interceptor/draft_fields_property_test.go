package interceptor

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "go.temporal.io/api/operatorservice/v1"
	protometapb "go.temporal.io/api/protometa/v1"
	_ "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/draft"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/testing/protocmp"
)

// TestDraftFieldsMatchDescriptors runs random requests and responses of every unary
// WorkflowService and OperatorService method through the draft interceptors with a
// random set of enabled features. The handler must receive, and the client must get,
// exactly the message with the fields of disabled features cleared, as decided from the
// draft annotations in the descriptors alone: a field needs its own feature and those of
// its type and of the messages that type is nested in.
func TestDraftFieldsMatchDescriptors(t *testing.T) {
	t.Parallel()
	methods := draftServiceMethods(t)
	features := apiDraftFeatures()
	var affected, unaffected []protoreflect.MethodDescriptor
	for _, method := range methods {
		if reachesDraft(method.Input(), nil) || reachesDraft(method.Output(), nil) {
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

		i := NewDraftFieldsInterceptor(func(string) []string { return enabled }, registry, log.NewNoopLogger(), draft.Omit)
		info := &grpc.UnaryServerInfo{FullMethod: fmt.Sprintf("/%s/%s", method.Parent().FullName(), method.Name())}
		var received proto.Message
		returned, err := i.OutgoingIntercept(context.Background(), proto.Clone(request), info, func(ctx context.Context, req any) (any, error) {
			return i.IncomingIntercept(ctx, req, info, func(context.Context, any) (any, error) {
				received = proto.Clone(req.(proto.Message))
				return proto.Clone(response), nil
			})
		})
		if err != nil {
			t.Fatalf("seed %d, %s: %v", seed, info.FullMethod, err)
		}
		for _, check := range []struct {
			what           string
			sent, observed proto.Message
		}{{"request", request, received}, {"response", response, returned.(proto.Message)}} {
			want := proto.Clone(check.sent)
			stripDisabled(want.ProtoReflect(), enabled)
			if !proto.Equal(want, check.sent) {
				stripped[check.what]++
			}
			if diff := cmp.Diff(want, check.observed, protocmp.Transform()); diff != "" {
				t.Fatalf("seed %d, %s %s with features %v differs from the descriptor reference (-want +got):\n%s",
					seed, info.FullMethod, check.what, enabled, diff)
			}
		}
	}
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
	for _, name := range []protoreflect.FullName{"temporal.api.workflowservice.v1.WorkflowService", "temporal.api.operatorservice.v1.OperatorService"} {
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
	var visit func(protoreflect.MessageDescriptors)
	visit = func(messages protoreflect.MessageDescriptors) {
		for i := 0; i < messages.Len(); i++ {
			message := messages.Get(i)
			features = append(features, typeDraftFeatures(message)...)
			for j := 0; j < message.Fields().Len(); j++ {
				features = append(features, fieldDraftFeatures(message.Fields().Get(j))...)
			}
			visit(message.Messages())
		}
	}
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if strings.HasPrefix(string(file.Package()), "temporal.api.") {
			visit(file.Messages())
		}
		return true
	})
	slices.Sort(features)
	return slices.Compact(features)
}

// typeDraftFeatures returns the draft_message features of a message or enum and of the
// messages it is nested in.
func typeDraftFeatures(descriptor protoreflect.Descriptor) []string {
	var features []string
	for ; descriptor != nil; descriptor = descriptor.Parent() {
		if message, ok := descriptor.(protoreflect.MessageDescriptor); ok {
			if feature, _ := proto.GetExtension(message.Options(), protometapb.E_DraftMessage).(string); feature != "" {
				features = append(features, feature)
			}
		}
	}
	return features
}

// fieldDraftFeatures returns the features a field needs: its own and those of the
// message or enum it holds, or of a map field's value.
func fieldDraftFeatures(field protoreflect.FieldDescriptor) []string {
	var features []string
	if feature, _ := proto.GetExtension(field.Options(), protometapb.E_Draft).(string); feature != "" {
		features = append(features, feature)
	}
	if field.IsMap() {
		field = field.MapValue()
	}
	if field.Message() != nil {
		features = append(features, typeDraftFeatures(field.Message())...)
	} else if field.Enum() != nil {
		features = append(features, typeDraftFeatures(field.Enum())...)
	}
	return features
}

// reachesDraft reports whether a message of this type can hold a draft field.
func reachesDraft(message protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) bool {
	if seen == nil {
		seen = map[protoreflect.FullName]bool{}
	}
	if seen[message.FullName()] {
		return false
	}
	seen[message.FullName()] = true
	for i := 0; i < message.Fields().Len(); i++ {
		field := message.Fields().Get(i)
		if len(fieldDraftFeatures(field)) > 0 {
			return true
		}
		if field.IsMap() {
			field = field.MapValue()
		}
		if field.Message() != nil && reachesDraft(field.Message(), seen) {
			return true
		}
	}
	return false
}

// leadsToDraft reports whether field is draft or holds messages that can hold one.
func leadsToDraft(field protoreflect.FieldDescriptor) bool {
	if len(fieldDraftFeatures(field)) > 0 {
		return true
	}
	if field.IsMap() {
		field = field.MapValue()
	}
	return field.Message() != nil && reachesDraft(field.Message(), nil)
}

// stripDisabled clears every field that needs a feature outside enabled, at any depth.
func stripDisabled(m protoreflect.Message, enabled []string) {
	var disabled []protoreflect.FieldDescriptor
	m.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		for _, feature := range fieldDraftFeatures(field) {
			if !slices.Contains(enabled, feature) {
				disabled = append(disabled, field)
				return true
			}
		}
		switch {
		case field.IsList() && field.Message() != nil:
			for i := 0; i < value.List().Len(); i++ {
				stripDisabled(value.List().Get(i).Message(), enabled)
			}
		case field.IsMap() && field.MapValue().Message() != nil:
			value.Map().Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
				stripDisabled(entry.Message(), enabled)
				return true
			})
		case field.Message() != nil && !field.IsList() && !field.IsMap():
			stripDisabled(value.Message(), enabled)
		default:
			// Scalars and lists, maps of scalars hold no draft fields.
		}
		return true
	})
	for _, field := range disabled {
		m.Clear(field)
	}
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
