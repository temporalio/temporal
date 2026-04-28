package umpire

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type RPCRegistry struct {
	mu    sync.RWMutex
	specs map[string]RPCSpec
}

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
	if isNilProtoMessage(message) {
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

func (r *RPCRegistry) Register(spec RPCSpec) struct{} {
	if err := r.register(spec); err != nil {
		panic(err)
	}
	return struct{}{}
}

func (r *RPCRegistry) Lookup(method string) (RPCSpec, bool) {
	if r == nil {
		return RPCSpec{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	spec, ok := r.specs[method]
	if !ok {
		return RPCSpec{}, false
	}
	return copyRPCSpec(spec), true
}

func (r *RPCRegistry) Specs() []RPCSpec {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	specs := make([]RPCSpec, 0, len(r.specs))
	for _, spec := range r.specs {
		specs = append(specs, copyRPCSpec(spec))
	}
	sort.Slice(specs, func(i, j int) bool {
		return specs[i].Method < specs[j].Method
	})
	return specs
}

func (r *RPCRegistry) HasMutableRequest(method string) bool {
	spec, ok := r.Lookup(method)
	if !ok {
		return false
	}
	return spec.hasMutableRequest()
}

func (r *RPCRegistry) register(spec RPCSpec) error {
	if err := spec.validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.specs == nil {
		r.specs = make(map[string]RPCSpec)
	}
	if _, ok := r.specs[spec.Method]; ok {
		return fmt.Errorf("umpire: duplicate RPC spec for %s", spec.Method)
	}
	r.specs[spec.Method] = copyRPCSpec(spec)
	return nil
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

type RPCRegistryStrategyOption func(*rpcRegistryStrategyConfig)

type rpcRegistryStrategyConfig struct {
	methodFilter                 func(string) bool
	recordUnregisteredViolations bool
}

func WithRPCRegistryMethodFilter(filter func(string) bool) RPCRegistryStrategyOption {
	return func(config *rpcRegistryStrategyConfig) {
		config.methodFilter = filter
	}
}

func WithUnregisteredRPCViolations() RPCRegistryStrategyOption {
	return func(config *rpcRegistryStrategyConfig) {
		config.recordUnregisteredViolations = true
	}
}

func RPCRegistryStrategy(u *Umpire, registry *RPCRegistry, options ...RPCRegistryStrategyOption) Strategy {
	var config rpcRegistryStrategyConfig
	for _, option := range options {
		option(&config)
	}
	var currentT atomic.Pointer[T]
	recordIssue := func(t *T, issue *RPCRegistryIssue) {
		if t != nil {
			t.Logf("rpc registry: %s method=%s expected=%s actual=%s", issue.Message, issue.Method, issue.Expected, issue.Actual)
		}
		if u != nil {
			u.Record(issue)
		}
	}
	validateMessage := func(t *T, method string, role string, expected protoreflect.FullName, value any) {
		if expected == "" {
			return
		}
		actual, ok := rpcValueMessageName(value)
		if !ok {
			recordIssue(t, &RPCRegistryIssue{
				Method:   method,
				Message:  role + " is not a proto message",
				Expected: string(expected),
				Actual:   fmt.Sprintf("%T", value),
			})
			return
		}
		if actual != expected {
			recordIssue(t, &RPCRegistryIssue{
				Method:   method,
				Message:  role + " type mismatch",
				Expected: string(expected),
				Actual:   string(actual),
			})
		}
	}
	return Strategy{
		Name: "rpc-registry",
		Client: func(
			ctx context.Context,
			method string,
			req, reply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			opts ...grpc.CallOption,
		) error {
			t := currentT.Load()
			if config.methodFilter != nil && !config.methodFilter(method) {
				return invoker(ctx, method, req, reply, cc, opts...)
			}
			spec, ok := registry.Lookup(method)
			if !ok {
				if config.recordUnregisteredViolations {
					recordIssue(t, &RPCRegistryIssue{
						Method:  method,
						Message: "unregistered RPC",
						Actual:  fmt.Sprintf("%T", req),
					})
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			}
			validateMessage(t, method, "request", spec.Request, req)
			if t != nil && !skipRPCFieldAssertions(ctx) {
				spec.assertFields(t, method, "request", req, spec.RequestFields)
			}
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				validateMessage(t, method, "response", spec.Response, reply)
				if t != nil && !skipRPCFieldAssertions(ctx) {
					spec.assertFields(t, method, "response", reply, spec.ResponseFields)
				}
			}
			return err
		},
		SetT: func(t *T) { currentT.Store(t) },
	}
}

type RPCRegistryIssue struct {
	Method   string
	Message  string
	Expected string
	Actual   string
}

func (i *RPCRegistryIssue) Key() string {
	if i == nil {
		return ""
	}
	return i.Method
}

func RPCRegistryRule() SafetyRule {
	return SafetyRule{
		Name: "rpc-registry",
		Check: func(ctx *RuleContext, history []*Record) {
			for _, rec := range history {
				issue, ok := rec.Fact.(*RPCRegistryIssue)
				if !ok {
					continue
				}
				ctx.Violate(issue.Message, map[string]string{
					"method":   issue.Method,
					"expected": issue.Expected,
					"actual":   issue.Actual,
				})
			}
		},
	}
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
	if !ok || isNilProtoMessage(message) {
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

func protoMessageName(message proto.Message) protoreflect.FullName {
	if message == nil || isNilProtoMessage(message) {
		return ""
	}
	return message.ProtoReflect().Descriptor().FullName()
}

func rpcValueMessageName(value any) (protoreflect.FullName, bool) {
	message, ok := value.(proto.Message)
	if !ok || isNilProtoMessage(message) {
		return "", false
	}
	return message.ProtoReflect().Descriptor().FullName(), true
}

func rpcMessageDescriptor(name protoreflect.FullName) (protoreflect.MessageDescriptor, error) {
	message, err := protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		return nil, fmt.Errorf("message %s is not registered: %w", name, err)
	}
	return message.Descriptor(), nil
}

func resolveRPCFieldPath(
	message protoreflect.FullName,
	field RPCFieldSpec,
) (protoMutationPath, error) {
	if field.Proto != nil {
		return append(protoMutationPath(nil), field.Proto...), nil
	}
	descriptor, err := rpcMessageDescriptor(message)
	if err != nil {
		return nil, err
	}
	return resolveProtoMutationPath(descriptor, field.Path)
}

func resolveRPCFieldPathForDescriptor(
	descriptor protoreflect.MessageDescriptor,
	field RPCFieldSpec,
) (protoMutationPath, error) {
	if field.Proto != nil {
		return append(protoMutationPath(nil), field.Proto...), nil
	}
	return resolveProtoMutationPath(descriptor, field.Path)
}

func resolveProtoMutationPath(
	descriptor protoreflect.MessageDescriptor,
	fieldPath string,
) (protoMutationPath, error) {
	parts := strings.Split(fieldPath, ".")
	path := make(protoMutationPath, 0, len(parts))
	for i, part := range parts {
		field := descriptor.Fields().ByName(protoreflect.Name(part))
		if field == nil {
			return nil, errors.New("field not found")
		}
		path = append(path, field)
		if i == len(parts)-1 {
			return path, nil
		}
		if field.IsList() || field.IsMap() {
			return nil, errors.New("field is repeated or map")
		}
		if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
			return nil, errors.New("field is not a message")
		}
		descriptor = field.Message()
	}
	return nil, errors.New("field path is empty")
}

func rpcFieldValue(
	message protoreflect.Message,
	path protoMutationPath,
) (protoreflect.FieldDescriptor, protoreflect.Value, error) {
	if len(path) == 0 {
		return nil, protoreflect.Value{}, errors.New("field path is empty")
	}
	for _, field := range path[:len(path)-1] {
		if field.IsList() || field.IsMap() {
			return nil, protoreflect.Value{}, fmt.Errorf("field %s is repeated or map", field.FullName())
		}
		if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
			return nil, protoreflect.Value{}, fmt.Errorf("field %s is not a message", field.FullName())
		}
		message = message.Get(field).Message()
	}
	field := path[len(path)-1]
	return field, message.Get(field), nil
}

func validateRPCMutationPath(path protoMutationPath) error {
	if len(path) == 0 {
		return errors.New("field path is empty")
	}
	for _, field := range path[:len(path)-1] {
		if field.IsList() || field.IsMap() {
			return fmt.Errorf("field %s is repeated or map", field.FullName())
		}
		if field.ContainingOneof() != nil {
			return fmt.Errorf("field %s is in a oneof", field.FullName())
		}
	}
	field := path[len(path)-1]
	if field.IsList() || field.IsMap() {
		return fmt.Errorf("field %s is repeated or map", field.FullName())
	}
	if (field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind) && !isDurationField(field) {
		return fmt.Errorf("field %s is a message", field.FullName())
	}
	return nil
}

func isDurationField(field protoreflect.FieldDescriptor) bool {
	return field.Kind() == protoreflect.MessageKind &&
		field.Message() != nil &&
		field.Message().FullName() == durationProtoFullName
}
