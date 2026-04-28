package umpire

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"pgregory.net/rapid"
)

type RequestMutations struct {
	currentT             atomic.Pointer[T]
	enabled              bool
	probabilityNumerator int
	probabilityDenom     int
	maxProtoMutations    int
	maxProtoDepth        int
	maxStringRunes       int
	maxBytesLen          int
}

type RequestMutationOption func(*RequestMutations)

func NewRequestMutations(options ...RequestMutationOption) *RequestMutations {
	mutations := &RequestMutations{
		enabled:              true,
		probabilityNumerator: 1,
		probabilityDenom:     2,
		maxProtoMutations:    1,
		maxProtoDepth:        4,
		maxStringRunes:       24,
		maxBytesLen:          32,
	}
	for _, option := range options {
		option(mutations)
	}
	return mutations
}

func WithRequestMutationProbability(numerator, denominator int) RequestMutationOption {
	return func(m *RequestMutations) {
		if denominator <= 0 {
			denominator = 1
		}
		if numerator < 0 {
			numerator = 0
		}
		if numerator > denominator {
			numerator = denominator
		}
		m.probabilityNumerator = numerator
		m.probabilityDenom = denominator
	}
}

func WithMaxProtoRequestMutations(maxMutations int) RequestMutationOption {
	return func(m *RequestMutations) {
		if maxMutations < 1 {
			maxMutations = 1
		}
		m.maxProtoMutations = maxMutations
	}
}

func (m *RequestMutations) SetRequestMutationT(t *T) {
	m.currentT.Store(t)
}

func (m *RequestMutations) UnaryClientInterceptor(
	shouldMutateMethod func(string) bool,
	fallbackOnMutationError func(error) bool,
) grpc.UnaryClientInterceptor {
	return m.unaryClientInterceptor(
		func(method string, req proto.Message) ([]protoMutationCandidate, bool, error) {
			if shouldMutateMethod != nil && !shouldMutateMethod(method) {
				return nil, false, nil
			}
			return collectProtoMutationFields(req.ProtoReflect().Descriptor(), m.maxProtoDepth), true, nil
		},
		fallbackOnMutationError,
	)
}

func (m *RequestMutations) UnaryClientInterceptorForRegistry(
	registry *RPCRegistry,
	fallbackOnMutationError func(error) bool,
) grpc.UnaryClientInterceptor {
	return m.unaryClientInterceptor(
		func(method string, req proto.Message) ([]protoMutationCandidate, bool, error) {
			spec, ok := registry.Lookup(method)
			if !ok || !spec.hasMutableRequest() {
				return nil, false, nil
			}
			candidates, err := spec.requestMutationCandidates(req.ProtoReflect().Descriptor())
			return candidates, true, err
		},
		fallbackOnMutationError,
	)
}

func (m *RequestMutations) unaryClientInterceptor(
	candidates func(method string, req proto.Message) ([]protoMutationCandidate, bool, error),
	fallbackOnMutationError func(error) bool,
) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		t := m.currentT.Load()
		reqMessage, ok := req.(proto.Message)
		if t == nil || !ok || isNilProtoMessage(reqMessage) {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		mutationCandidates, ok, err := candidates(method, reqMessage)
		if err != nil {
			t.Fatalf("request mutation candidates for %s: %v", method, err)
		}
		if !ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		originalReq := proto.Clone(reqMessage)
		mutatedFields := mutateProtoMessageWithCandidates(t, m, protoRequestMutationLabel(method), reqMessage, mutationCandidates)
		if len(mutatedFields) == 0 {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		t.Logf("mutation: method=%s fields=%s", method, strings.Join(mutatedFields, ","))

		err = invoker(withRPCFieldAssertionsSkipped(ctx), method, req, reply, cc, opts...)
		if err == nil || fallbackOnMutationError == nil || !fallbackOnMutationError(err) {
			return err
		}

		t.Logf("mutation fallback: method=%s err=%v retry=original-request", method, err)
		restoreProtoMessage(reqMessage, originalReq)
		if replyMessage, ok := reply.(proto.Message); ok && !isNilProtoMessage(replyMessage) {
			proto.Reset(replyMessage)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func MutateProtoRequest[Req proto.Message](
	t *T,
	mutations *RequestMutations,
	label string,
	req Req,
) (Req, bool) {
	t.Helper()
	if !mutations.shouldMutate(t, label) || isNilProto(req) {
		return req, false
	}

	clonedMessage := proto.Clone(req)
	clonedReq, ok := clonedMessage.(Req)
	if !ok {
		t.Fatalf("proto clone of %T returned %T", req, clonedMessage)
		return req, false
	}

	mutatedFields := mutateProtoMessage(t, mutations, label, clonedReq)
	if len(mutatedFields) == 0 {
		return req, false
	}
	t.Logf("mutation: label=%s fields=%s", label, strings.Join(mutatedFields, ","))
	return clonedReq, true
}

func mutateProtoMessage(
	t *T,
	mutations *RequestMutations,
	label string,
	req proto.Message,
) []string {
	msg := req.ProtoReflect()
	candidates := collectProtoMutationFields(msg.Descriptor(), mutations.maxProtoDepth)
	return mutateProtoMessageWithCandidates(t, mutations, label, req, candidates)
}

func mutateProtoMessageWithCandidates(
	t *T,
	mutations *RequestMutations,
	label string,
	req proto.Message,
	candidates []protoMutationCandidate,
) []string {
	if len(candidates) == 0 {
		return nil
	}

	maxMutations := min(mutations.maxProtoMutations, len(candidates))
	count := drawGen(t, label+".mutationCount", rapid.IntRange(1, maxMutations))
	mutatedFields := make([]string, 0, count)
	msg := req.ProtoReflect()
	for i := range count {
		mutationLabel := fmt.Sprintf("%s.mutation.%d", label, i)
		candidate := candidates[drawGen(t, mutationLabel+".candidate", rapid.IntRange(0, len(candidates)-1))]
		applyProtoMutation(t, mutations, mutationLabel, msg, candidate)
		mutatedFields = append(mutatedFields, candidate.Label())
	}
	return mutatedFields
}

func (m *RequestMutations) shouldMutate(t *T, label string) bool {
	if !m.enabled || m.probabilityNumerator <= 0 {
		return false
	}
	if m.probabilityNumerator >= m.probabilityDenom {
		return true
	}
	draw := drawGen(t, label+".mutationEnabled", rapid.IntRange(1, m.probabilityDenom))
	return draw <= m.probabilityNumerator
}

func isNilProto[M proto.Message](msg M) bool {
	value := reflect.ValueOf(msg)
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilProtoMessage(msg proto.Message) bool {
	value := reflect.ValueOf(msg)
	if !value.IsValid() {
		return true
	}
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func restoreProtoMessage(dst proto.Message, src proto.Message) {
	proto.Reset(dst)
	proto.Merge(dst, src)
}

func protoRequestMutationLabel(method string) string {
	method = strings.TrimPrefix(method, "/")
	method = strings.ReplaceAll(method, "/", ".")
	return "grpc." + method
}

type protoMutationPath []protoreflect.FieldDescriptor

type protoMutationField struct {
	Path     protoMutationPath
	Generate RPCFieldValueGenerator
}

type protoMutationCandidate struct {
	Name   string
	Fields []protoMutationField
}

func (c protoMutationCandidate) Label() string {
	if c.Name != "" {
		return c.Name
	}
	fields := make([]string, 0, len(c.Fields))
	for _, field := range c.Fields {
		fields = append(fields, protoPathLabel(field.Path))
	}
	return strings.Join(fields, "+")
}

func collectProtoMutationFields(
	descriptor protoreflect.MessageDescriptor,
	maxDepth int,
) []protoMutationCandidate {
	var paths []protoMutationPath
	collectProtoMutationFieldsAt(&paths, nil, descriptor, maxDepth)
	candidates := make([]protoMutationCandidate, 0, len(paths))
	for _, path := range paths {
		candidates = append(candidates, protoMutationCandidate{
			Name: protoPathLabel(path),
			Fields: []protoMutationField{{
				Path: path,
			}},
		})
	}
	return candidates
}

func collectProtoMutationFieldsAt(
	paths *[]protoMutationPath,
	prefix protoMutationPath,
	descriptor protoreflect.MessageDescriptor,
	depthRemaining int,
) {
	fields := descriptor.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		if field.IsList() || field.IsMap() || field.ContainingOneof() != nil {
			continue
		}
		path := append(append(protoMutationPath(nil), prefix...), field)
		switch field.Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			if depthRemaining > 0 {
				collectProtoMutationFieldsAt(paths, path, field.Message(), depthRemaining-1)
			}
		default:
			*paths = append(*paths, path)
		}
	}
}

func applyProtoMutation(
	t *T,
	mutations *RequestMutations,
	label string,
	msg protoreflect.Message,
	candidate protoMutationCandidate,
) {
	for _, mutationField := range candidate.Fields {
		applyProtoMutationField(t, mutations, label, msg, mutationField)
	}
}

func applyProtoMutationField(
	t *T,
	mutations *RequestMutations,
	label string,
	msg protoreflect.Message,
	mutationField protoMutationField,
) {
	path := mutationField.Path
	for _, field := range path[:len(path)-1] {
		msg = msg.Mutable(field).Message()
	}
	field := path[len(path)-1]
	valueLabel := label + "." + protoPathLabel(path)
	if mutationField.Generate != nil {
		msg.Set(field, mutationField.Generate(t, valueLabel, field))
		return
	}
	msg.Set(field, mutatedProtoValue(t, mutations, valueLabel, field))
}

func protoPathLabel(path protoMutationPath) string {
	parts := make([]string, 0, len(path))
	for _, field := range path {
		parts = append(parts, string(field.Name()))
	}
	return strings.Join(parts, ".")
}

func mutatedProtoValue(
	t *T,
	mutations *RequestMutations,
	label string,
	field protoreflect.FieldDescriptor,
) protoreflect.Value {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(drawGen(t, label, rapid.Bool()))
	case protoreflect.EnumKind:
		return protoreflect.ValueOfEnum(mutatedEnum(t, label, field.Enum()))
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(drawGen(t, label, rapid.Int32()))
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(drawGen(t, label, rapid.Int64()))
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(drawGen(t, label, rapid.Uint32()))
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(drawGen(t, label, rapid.Uint64()))
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(drawGen(t, label, rapid.Float32()))
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(drawGen(t, label, rapid.Float64()))
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(mutatedString(t, mutations, label))
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes(drawGen(t, label, rapid.SliceOfN(rapid.Uint8(), 0, mutations.maxBytesLen)))
	case protoreflect.MessageKind:
		if isDurationField(field) {
			return protoreflect.ValueOfMessage(durationpb.New(time.Duration(drawGen(t, label, rapid.Int64())).Abs()).ProtoReflect())
		}
		fallthrough
	default:
		t.Fatalf("unsupported proto mutation field %s kind %s", field.FullName(), field.Kind())
		return field.Default()
	}
}

func mutatedEnum(t *T, label string, descriptor protoreflect.EnumDescriptor) protoreflect.EnumNumber {
	values := descriptor.Values()
	if values.Len() == 0 || drawGen(t, label+".unknown", rapid.Bool()) {
		return protoreflect.EnumNumber(drawGen(t, label+".number", rapid.Int32()))
	}
	numbers := make([]protoreflect.EnumNumber, 0, values.Len())
	for i := range values.Len() {
		numbers = append(numbers, values.Get(i).Number())
	}
	return Draw(t, label+".known", numbers)
}

func mutatedString(t *T, mutations *RequestMutations, label string) string {
	return drawGen(t, label, rapid.OneOf(
		rapid.Just(""),
		rapid.StringMatching(`[A-Za-z0-9_.:/-]{1,32}`),
		rapid.StringN(0, mutations.maxStringRunes, mutations.maxStringRunes*4),
	))
}
