package umpire

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type RPCSpec struct {
	Method         string
	Request        protoreflect.FullName
	Response       protoreflect.FullName
	RequestFields  []RPCFieldSpec
	ResponseFields []RPCFieldSpec
	Mutations      []RPCMutationSpec
}

type RPCSpecOption func(*RPCSpec)

func RPCSpecFor(method string, request proto.Message, response proto.Message, options ...RPCSpecOption) RPCSpec {
	spec := RPCSpec{
		Method:   method,
		Request:  protoMessageName(request),
		Response: protoMessageName(response),
	}
	for _, option := range options {
		option(&spec)
	}
	return spec
}

type RPCSpecBuilder[Req proto.Message, Resp proto.Message] struct {
	spec RPCSpec
}

func NewRPCSpec[Req proto.Message, Resp proto.Message](
	method string,
	request Req,
	response Resp,
) *RPCSpecBuilder[Req, Resp] {
	return &RPCSpecBuilder[Req, Resp]{
		spec: RPCSpec{
			Method:   method,
			Request:  protoMessageName(request),
			Response: protoMessageName(response),
		},
	}
}

func (b *RPCSpecBuilder[Req, Resp]) RequestFields(fields ...TypedRPCField[Req]) *RPCSpecBuilder[Req, Resp] {
	for _, field := range fields {
		b.spec.RequestFields = append(b.spec.RequestFields, field.spec)
	}
	return b
}

func (b *RPCSpecBuilder[Req, Resp]) ResponseFields(fields ...TypedRPCField[Resp]) *RPCSpecBuilder[Req, Resp] {
	for _, field := range fields {
		b.spec.ResponseFields = append(b.spec.ResponseFields, field.spec)
	}
	return b
}

func (b *RPCSpecBuilder[Req, Resp]) Mutation(name string, fields ...RPCFieldRef[Req]) *RPCSpecBuilder[Req, Resp] {
	mutation := RPCMutationSpec{Name: name}
	for _, field := range fields {
		mutation.Fields = append(mutation.Fields, field.rpcFieldSpec())
	}
	b.spec.Mutations = append(b.spec.Mutations, mutation)
	return b
}

func (b *RPCSpecBuilder[Req, Resp]) Build() RPCSpec {
	return copyRPCSpec(b.spec)
}

func WithRPCRequestFields(fields ...RPCFieldSpec) RPCSpecOption {
	return func(spec *RPCSpec) {
		spec.RequestFields = append(spec.RequestFields, fields...)
	}
}

func WithRPCResponseFields(fields ...RPCFieldSpec) RPCSpecOption {
	return func(spec *RPCSpec) {
		spec.ResponseFields = append(spec.ResponseFields, fields...)
	}
}

func WithRPCMutation(name string, fields ...string) RPCSpecOption {
	return func(spec *RPCSpec) {
		mutation := RPCMutationSpec{Name: name}
		for _, field := range fields {
			mutation.Fields = append(mutation.Fields, RPCField(field))
		}
		spec.Mutations = append(spec.Mutations, mutation)
	}
}

type RPCFieldSpec struct {
	Path       string
	Message    protoreflect.FullName
	Proto      protoMutationPath
	Role       RPCFieldRole
	Mutation   RPCFieldMutation
	Generate   RPCFieldValueGenerator
	Assertions []RPCFieldAssertion
}

type RPCFieldRef[M proto.Message] struct {
	message protoreflect.FullName
	path    protoMutationPath
	label   string
}

type TypedRPCField[M proto.Message] struct {
	spec RPCFieldSpec
}

type RPCFieldOption func(*RPCFieldSpec)

type RPCFieldRole string

const (
	RPCFieldRoleStable           RPCFieldRole = "stable"
	RPCFieldRoleID               RPCFieldRole = "id"
	RPCFieldRoleTime             RPCFieldRole = "time"
	RPCFieldRoleNonDeterministic RPCFieldRole = "non-deterministic"
	RPCFieldRoleOpaque           RPCFieldRole = "opaque"
)

type RPCFieldMutation string

const (
	RPCFieldMutationNone   RPCFieldMutation = ""
	RPCFieldMutationRandom RPCFieldMutation = "random"
)

const durationProtoFullName protoreflect.FullName = "google.protobuf.Duration"

type RPCFieldValueGenerator func(*T, string, protoreflect.FieldDescriptor) protoreflect.Value

type RPCFieldAssertion func(*T, string, protoreflect.FieldDescriptor, protoreflect.Value)

func RPCField(path string, options ...RPCFieldOption) RPCFieldSpec {
	field := RPCFieldSpec{
		Path: path,
		Role: RPCFieldRoleStable,
	}
	for _, option := range options {
		option(&field)
	}
	return field
}

func MutableRPCField(path string, options ...RPCFieldOption) RPCFieldSpec {
	field := RPCField(path, options...)
	field.Mutation = RPCFieldMutationRandom
	return field
}

func RPCFieldOf[M proto.Message](ref RPCFieldRef[M], options ...RPCFieldOption) TypedRPCField[M] {
	field := ref.rpcFieldSpec()
	for _, option := range options {
		option(&field)
	}
	return TypedRPCField[M]{spec: field}
}

func MutableRPCFieldOf[M proto.Message](ref RPCFieldRef[M], options ...RPCFieldOption) TypedRPCField[M] {
	field := RPCFieldOf(ref, options...)
	field.spec.Mutation = RPCFieldMutationRandom
	return field
}

func NewRPCFieldRef[M proto.Message](message M, fields ...protoreflect.FieldNumber) RPCFieldRef[M] {
	if isNilProto(message) {
		panic("umpire: RPC field ref requires a non-nil proto message")
	}
	descriptor := message.ProtoReflect().Descriptor()
	path := make(protoMutationPath, 0, len(fields))
	for i, fieldNumber := range fields {
		field := descriptor.Fields().ByNumber(fieldNumber)
		if field == nil {
			panic(fmt.Sprintf("umpire: field number %d not found in %s", fieldNumber, descriptor.FullName()))
		}
		path = append(path, field)
		if i == len(fields)-1 {
			return RPCFieldRef[M]{
				message: message.ProtoReflect().Descriptor().FullName(),
				path:    path,
				label:   protoPathLabel(path),
			}
		}
		if field.IsList() || field.IsMap() {
			panic(fmt.Sprintf("umpire: field %s is repeated or map", field.FullName()))
		}
		if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
			panic(fmt.Sprintf("umpire: field %s is not a message", field.FullName()))
		}
		descriptor = field.Message()
	}
	panic("umpire: RPC field ref requires at least one field")
}

func (r RPCFieldRef[M]) rpcFieldSpec() RPCFieldSpec {
	return RPCFieldSpec{
		Path:    r.label,
		Message: r.message,
		Proto:   append(protoMutationPath(nil), r.path...),
		Role:    RPCFieldRoleStable,
	}
}

func WithRPCFieldRole(role RPCFieldRole) RPCFieldOption {
	return func(field *RPCFieldSpec) {
		field.Role = role
	}
}

func WithRPCFieldMutation(mutation RPCFieldMutation) RPCFieldOption {
	return func(field *RPCFieldSpec) {
		field.Mutation = mutation
	}
}

func WithRPCFieldGenerator(generator RPCFieldValueGenerator) RPCFieldOption {
	return func(field *RPCFieldSpec) {
		field.Generate = generator
	}
}

func WithRPCFieldAssertion(assertions ...RPCFieldAssertion) RPCFieldOption {
	return func(field *RPCFieldSpec) {
		field.Assertions = append(field.Assertions, assertions...)
	}
}

type RPCMutationSpec struct {
	Name   string
	Fields []RPCFieldSpec
}

type rpcFieldAssertionsSkipKey struct{}

func withRPCFieldAssertionsSkipped(ctx context.Context) context.Context {
	return context.WithValue(ctx, rpcFieldAssertionsSkipKey{}, true)
}

func skipRPCFieldAssertions(ctx context.Context) bool {
	skip, _ := ctx.Value(rpcFieldAssertionsSkipKey{}).(bool)
	return skip
}

func (s RPCSpec) validate() error {
	if s.Method == "" {
		return errors.New("umpire: RPC spec method is required")
	}
	if err := validateRPCFields("request", s.Request, s.RequestFields); err != nil {
		return fmt.Errorf("umpire: RPC spec %s: %w", s.Method, err)
	}
	if err := validateRPCFields("response", s.Response, s.ResponseFields); err != nil {
		return fmt.Errorf("umpire: RPC spec %s: %w", s.Method, err)
	}

	requestFields := make(map[string]RPCFieldSpec, len(s.RequestFields))
	for _, field := range s.RequestFields {
		requestFields[field.Path] = field
	}
	mutationNames := make(map[string]struct{}, len(s.Mutations))
	for _, mutation := range s.Mutations {
		if mutation.Name == "" {
			return fmt.Errorf("umpire: RPC spec %s: mutation name is required", s.Method)
		}
		if _, ok := mutationNames[mutation.Name]; ok {
			return fmt.Errorf("umpire: RPC spec %s: duplicate mutation %s", s.Method, mutation.Name)
		}
		mutationNames[mutation.Name] = struct{}{}
		if len(mutation.Fields) == 0 {
			return fmt.Errorf("umpire: RPC spec %s: mutation %s has no fields", s.Method, mutation.Name)
		}
		for _, field := range mutation.Fields {
			requestField, ok := requestFields[field.Path]
			if !ok {
				return fmt.Errorf("umpire: RPC spec %s: mutation %s references unknown request field %s", s.Method, mutation.Name, field.Path)
			}
			path, err := resolveRPCFieldPath(s.Request, requestField)
			if err != nil {
				return fmt.Errorf("umpire: RPC spec %s: mutation %s field %s: %w", s.Method, mutation.Name, field.Path, err)
			}
			if err := validateRPCMutationPath(path); err != nil {
				return fmt.Errorf("umpire: RPC spec %s: mutation %s field %s: %w", s.Method, mutation.Name, field.Path, err)
			}
		}
	}
	return nil
}

func validateRPCFields(kind string, message protoreflect.FullName, fields []RPCFieldSpec) error {
	if len(fields) == 0 {
		return nil
	}
	if message == "" {
		return fmt.Errorf("%s fields require a %s message type", kind, kind)
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field.Path == "" {
			return fmt.Errorf("%s field path is required", kind)
		}
		if _, ok := seen[field.Path]; ok {
			return fmt.Errorf("duplicate %s field %s", kind, field.Path)
		}
		seen[field.Path] = struct{}{}

		if field.Message != "" && field.Message != message {
			return fmt.Errorf("%s field %s belongs to %s", kind, field.Path, field.Message)
		}
		path, err := resolveRPCFieldPath(message, field)
		if err != nil {
			return fmt.Errorf("%s field %s: %w", kind, field.Path, err)
		}
		if field.Mutation == RPCFieldMutationRandom {
			if err := validateRPCMutationPath(path); err != nil {
				return fmt.Errorf("%s field %s: %w", kind, field.Path, err)
			}
		}
	}
	return nil
}

func (s RPCSpec) hasMutableRequest() bool {
	if len(s.Mutations) != 0 {
		return true
	}
	for _, field := range s.RequestFields {
		if field.Mutation != RPCFieldMutationNone {
			return true
		}
	}
	return false
}

func (s RPCSpec) requestMutationCandidates(descriptor protoreflect.MessageDescriptor) ([]protoMutationCandidate, error) {
	fieldsByPath := make(map[string]RPCFieldSpec, len(s.RequestFields))
	for _, field := range s.RequestFields {
		fieldsByPath[field.Path] = field
	}

	var candidates []protoMutationCandidate
	for _, field := range s.RequestFields {
		if field.Mutation == RPCFieldMutationNone {
			continue
		}
		path, err := resolveRPCFieldPathForDescriptor(descriptor, field)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, protoMutationCandidate{
			Name: field.Path,
			Fields: []protoMutationField{{
				Path:     path,
				Generate: field.Generate,
			}},
		})
	}
	for _, mutation := range s.Mutations {
		candidate := protoMutationCandidate{Name: mutation.Name}
		for _, mutationField := range mutation.Fields {
			field := fieldsByPath[mutationField.Path]
			path, err := resolveRPCFieldPathForDescriptor(descriptor, field)
			if err != nil {
				return nil, err
			}
			candidate.Fields = append(candidate.Fields, protoMutationField{
				Path:     path,
				Generate: field.Generate,
			})
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

func copyRPCSpec(spec RPCSpec) RPCSpec {
	spec.RequestFields = copyRPCFieldSpecs(spec.RequestFields)
	spec.ResponseFields = copyRPCFieldSpecs(spec.ResponseFields)
	spec.Mutations = append([]RPCMutationSpec(nil), spec.Mutations...)
	for i := range spec.Mutations {
		spec.Mutations[i].Fields = copyRPCFieldSpecs(spec.Mutations[i].Fields)
	}
	return spec
}

func copyRPCFieldSpecs(fields []RPCFieldSpec) []RPCFieldSpec {
	out := append([]RPCFieldSpec(nil), fields...)
	for i := range out {
		out[i].Proto = append(protoMutationPath(nil), out[i].Proto...)
		out[i].Assertions = append([]RPCFieldAssertion(nil), out[i].Assertions...)
	}
	return out
}

func (s RPCSpec) assertFields(
	t *T,
	method string,
	kind string,
	value any,
	fields []RPCFieldSpec,
) {
	message, ok := value.(proto.Message)
	if !ok || isNilProto(message) {
		return
	}
	for _, field := range fields {
		if len(field.Assertions) == 0 {
			continue
		}
		path, err := resolveRPCFieldPathForDescriptor(message.ProtoReflect().Descriptor(), field)
		if err != nil {
			t.Fatalf("rpc %s %s assertion field %s: %v", method, kind, field.Path, err)
		}
		descriptor, fieldValue, err := rpcFieldValue(message.ProtoReflect(), path)
		if err != nil {
			t.Fatalf("rpc %s %s assertion field %s: %v", method, kind, field.Path, err)
		}
		label := method + "." + kind + "." + field.Path
		for _, assertion := range field.Assertions {
			assertion(t, label, descriptor, fieldValue)
		}
	}
}
