package umpire

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"pgregory.net/rapid"
)

type fieldSpecState uint8

const (
	fieldSpecUnset fieldSpecState = iota
	fieldSpecIncluded
	fieldSpecIgnored
)

type ValueGenerator[V any] func(*T, string) V

type FieldSpec[V any] struct {
	state         fieldSpecState
	role          RPCFieldRole
	mutation      RPCFieldMutation
	generator     ValueGenerator[V]
	assertions    []FieldAssertion[V]
	mutationNames []string
}

type FieldOption[V any] func(*FieldSpec[V])
type FieldAssertion[V any] func(*T, V)

func Field[V any](options ...FieldOption[V]) FieldSpec[V] {
	spec := FieldSpec[V]{
		state: fieldSpecIncluded,
		role:  RPCFieldRoleStable,
	}
	for _, option := range options {
		option(&spec)
	}
	return spec
}

func Ignored[V any]() FieldOption[V] {
	return func(spec *FieldSpec[V]) {
		spec.state = fieldSpecIgnored
	}
}

func Mutable[V any]() FieldOption[V] {
	return func(spec *FieldSpec[V]) {
		spec.mutation = RPCFieldMutationRandom
	}
}

func Role[V any](role RPCFieldRole) FieldOption[V] {
	return func(spec *FieldSpec[V]) {
		spec.role = role
	}
}

func Mutation[V any](name string) FieldOption[V] {
	return func(spec *FieldSpec[V]) {
		spec.mutationNames = append(spec.mutationNames, name)
	}
}

func Generate[V any](generator ValueGenerator[V]) FieldOption[V] {
	return func(spec *FieldSpec[V]) {
		spec.generator = generator
	}
}

func Assert[V any](assertion FieldAssertion[V]) FieldOption[V] {
	return func(spec *FieldSpec[V]) {
		spec.assertions = append(spec.assertions, assertion)
	}
}

type RPCMessageSpec[M proto.Message] interface {
	RPCMeta() RPCMessageMeta[M]
}

type RPCMessageMeta[M proto.Message] struct {
	Message M
	Fields  []RPCFieldMeta
}

type RPCFieldMeta struct {
	Name             string
	Path             string
	RefName          string
	DefaultGenerator RPCFieldValueGenerator
	Children         []RPCFieldMeta
}

type rpcFieldSpecValue interface {
	rpcState() fieldSpecState
	rpcMutationNames() []string
	rpcField(RPCFieldMeta) (RPCFieldSpec, bool, error)
}

func (f FieldSpec[V]) rpcState() fieldSpecState {
	return f.state
}

func (f FieldSpec[V]) rpcMutationNames() []string {
	return f.mutationNames
}

func (f FieldSpec[V]) rpcField(meta RPCFieldMeta) (RPCFieldSpec, bool, error) {
	switch f.state {
	case fieldSpecIncluded:
	case fieldSpecIgnored:
		if f.mutation != RPCFieldMutationNone || len(f.mutationNames) != 0 || f.generator != nil || len(f.assertions) != 0 {
			return RPCFieldSpec{}, false, fmt.Errorf("ignored field %s has active options", meta.Path)
		}
		return RPCFieldSpec{}, false, nil
	default:
		return RPCFieldSpec{}, false, fmt.Errorf("field %s is not set", meta.Path)
	}

	options := []RPCFieldOption{
		WithRPCFieldRole(f.role),
		WithRPCFieldMutation(f.mutation),
	}
	generator := meta.DefaultGenerator
	if f.generator != nil {
		generator = func(t *T, label string, field protoreflect.FieldDescriptor) protoreflect.Value {
			value, err := rpcValueOf(f.generator(t, label), field)
			if err != nil {
				t.Fatalf("generator for %s: %v", field.FullName(), err)
			}
			return value
		}
	}
	if generator != nil {
		options = append(options, WithRPCFieldGenerator(generator))
	}
	for _, assertion := range f.assertions {
		assertion := assertion
		options = append(options, WithRPCFieldAssertion(func(t *T, label string, field protoreflect.FieldDescriptor, value protoreflect.Value) {
			t.Helper()
			typedValue, err := rpcTypedValueOf[V](value, field)
			if err != nil {
				t.Fatalf("assertion value for %s: %v", label, err)
			}
			assertion(t, typedValue)
		}))
	}
	return RPCField(meta.Path, options...), true, nil
}

func BuildRPCSpec[
	Req proto.Message,
	Resp proto.Message,
	ReqSpec RPCMessageSpec[Req],
	RespSpec RPCMessageSpec[Resp],
](
	method string,
	reqSpec ReqSpec,
	respSpec RespSpec,
) RPCSpec {
	reqMeta := reqSpec.RPCMeta()
	respMeta := respSpec.RPCMeta()

	reqFields, mutations, err := collectRPCSpecFields(reqMeta, reqSpec, true)
	if err != nil {
		panic(fmt.Errorf("request spec for %s: %w", method, err))
	}
	respFields, respMutations, err := collectRPCSpecFields(respMeta, respSpec, false)
	if err != nil {
		panic(fmt.Errorf("response spec for %s: %w", method, err))
	}
	if len(respMutations) != 0 {
		panic(fmt.Errorf("response spec for %s declared mutations", method))
	}

	spec := RPCSpecFor(
		method,
		reqMeta.Message,
		respMeta.Message,
		WithRPCRequestFields(reqFields...),
		WithRPCResponseFields(respFields...),
	)
	spec.Mutations = append(spec.Mutations, mutations...)
	return spec
}

func collectRPCSpecFields[M proto.Message](
	meta RPCMessageMeta[M],
	spec any,
	allowMutations bool,
) ([]RPCFieldSpec, []RPCMutationSpec, error) {
	var fields []RPCFieldSpec
	mutationsByName := make(map[string][]RPCFieldSpec)
	specValue := reflect.ValueOf(spec)
	if specValue.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("spec %T is not a struct", spec)
	}
	for _, field := range meta.Fields {
		if err := collectRPCSpecField(&fields, mutationsByName, field, specValue, allowMutations); err != nil {
			return nil, nil, err
		}
	}

	names := make([]string, 0, len(mutationsByName))
	for name := range mutationsByName {
		names = append(names, name)
	}
	sort.Strings(names)

	mutations := make([]RPCMutationSpec, 0, len(names))
	for _, name := range names {
		mutations = append(mutations, RPCMutationSpec{
			Name:   name,
			Fields: mutationsByName[name],
		})
	}
	return fields, mutations, nil
}

func collectRPCSpecField(
	fields *[]RPCFieldSpec,
	mutationsByName map[string][]RPCFieldSpec,
	meta RPCFieldMeta,
	parent reflect.Value,
	allowMutations bool,
) error {
	value := parent.FieldByName(meta.Name)
	if !value.IsValid() {
		return fmt.Errorf("generated metadata references missing field %s", meta.Name)
	}
	if len(meta.Children) != 0 {
		ref := value.FieldByName(meta.RefName)
		if !ref.IsValid() {
			return fmt.Errorf("generated metadata references missing field %s.%s", meta.Name, meta.RefName)
		}
		refSpec, ok := ref.Interface().(rpcFieldSpecValue)
		if !ok {
			return fmt.Errorf("field %s.%s is not a field spec", meta.Name, meta.RefName)
		}
		if refSpec.rpcState() != fieldSpecUnset {
			return collectRPCLeafField(fields, mutationsByName, meta, refSpec, allowMutations)
		}
		for _, child := range meta.Children {
			if err := collectRPCSpecField(fields, mutationsByName, child, value, allowMutations); err != nil {
				return err
			}
		}
		return nil
	}

	spec, ok := value.Interface().(rpcFieldSpecValue)
	if !ok {
		return fmt.Errorf("field %s is not a field spec", meta.Name)
	}
	return collectRPCLeafField(fields, mutationsByName, meta, spec, allowMutations)
}

func collectRPCLeafField(
	fields *[]RPCFieldSpec,
	mutationsByName map[string][]RPCFieldSpec,
	meta RPCFieldMeta,
	spec rpcFieldSpecValue,
	allowMutations bool,
) error {
	field, include, err := spec.rpcField(meta)
	if err != nil {
		return err
	}
	mutationNames := spec.rpcMutationNames()
	if len(mutationNames) != 0 && !allowMutations {
		return fmt.Errorf("field %s declares request mutations in a response spec", meta.Path)
	}
	if !include {
		if len(mutationNames) != 0 {
			return fmt.Errorf("ignored field %s declares request mutations", meta.Path)
		}
		return nil
	}
	*fields = append(*fields, field)
	for _, name := range mutationNames {
		if name == "" {
			return fmt.Errorf("field %s declares an empty mutation name", meta.Path)
		}
		mutationsByName[name] = append(mutationsByName[name], field)
	}
	return nil
}

func RPCEnumValueGenerator[V ~int32](values ...V) RPCFieldValueGenerator {
	return func(t *T, label string, field protoreflect.FieldDescriptor) protoreflect.Value {
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(Draw(t, label, values)))
	}
}

func RPCDurationValueGenerator() RPCFieldValueGenerator {
	return func(t *T, label string, field protoreflect.FieldDescriptor) protoreflect.Value {
		return protoreflect.ValueOfMessage(durationpb.New(time.Duration(drawGen(t, label, rapid.Int64())).Abs()).ProtoReflect())
	}
}

func rpcValueOf(v any, field protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	if field.IsList() || field.IsMap() {
		return protoreflect.Value{}, errors.New("repeated and map fields are not supported")
	}
	switch field.Kind() {
	case protoreflect.BoolKind:
		value, ok := v.(bool)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected bool, got %T", v)
		}
		return protoreflect.ValueOfBool(value), nil
	case protoreflect.EnumKind:
		if value, ok := v.(protoreflect.Enum); ok {
			return protoreflect.ValueOfEnum(value.Number()), nil
		}
		value := reflect.ValueOf(v)
		if !value.IsValid() {
			return protoreflect.Value{}, errors.New("expected enum, got nil")
		}
		switch value.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(value.Int())), nil
		default:
			return protoreflect.Value{}, fmt.Errorf("expected enum, got %T", v)
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		value, ok := v.(int32)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected int32, got %T", v)
		}
		return protoreflect.ValueOfInt32(value), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		value, ok := v.(int64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected int64, got %T", v)
		}
		return protoreflect.ValueOfInt64(value), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		value, ok := v.(uint32)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected uint32, got %T", v)
		}
		return protoreflect.ValueOfUint32(value), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		value, ok := v.(uint64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected uint64, got %T", v)
		}
		return protoreflect.ValueOfUint64(value), nil
	case protoreflect.FloatKind:
		value, ok := v.(float32)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected float32, got %T", v)
		}
		return protoreflect.ValueOfFloat32(value), nil
	case protoreflect.DoubleKind:
		value, ok := v.(float64)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected float64, got %T", v)
		}
		return protoreflect.ValueOfFloat64(value), nil
	case protoreflect.StringKind:
		value, ok := v.(string)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected string, got %T", v)
		}
		return protoreflect.ValueOfString(value), nil
	case protoreflect.BytesKind:
		value, ok := v.([]byte)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("expected []byte, got %T", v)
		}
		return protoreflect.ValueOfBytes(value), nil
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if isDurationField(field) {
			value, ok := v.(time.Duration)
			if !ok {
				return protoreflect.Value{}, fmt.Errorf("expected time.Duration, got %T", v)
			}
			return protoreflect.ValueOfMessage(durationpb.New(value).ProtoReflect()), nil
		}
		value, ok := v.(proto.Message)
		if !ok || isNilProtoMessage(value) {
			return protoreflect.Value{}, fmt.Errorf("expected proto message, got %T", v)
		}
		return protoreflect.ValueOfMessage(value.ProtoReflect()), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported kind %s", field.Kind())
	}
}

func rpcTypedValueOf[V any](v protoreflect.Value, field protoreflect.FieldDescriptor) (V, error) {
	var zero V
	if !v.IsValid() {
		return zero, errors.New("value is invalid")
	}
	if field.IsList() || field.IsMap() {
		value, ok := v.Interface().(V)
		if !ok {
			return zero, fmt.Errorf("expected %s, got %T", reflect.TypeFor[V](), v.Interface())
		}
		return value, nil
	}

	var raw any
	switch field.Kind() {
	case protoreflect.BoolKind:
		raw = v.Bool()
	case protoreflect.EnumKind:
		target := reflect.TypeFor[V]()
		switch target.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			value := reflect.New(target).Elem()
			value.SetInt(int64(v.Enum()))
			return value.Interface().(V), nil
		default:
		}
		raw = v.Enum()
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		raw = int32(v.Int())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		raw = v.Int()
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		raw = uint32(v.Uint())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		raw = v.Uint()
	case protoreflect.FloatKind:
		raw = float32(v.Float())
	case protoreflect.DoubleKind:
		raw = v.Float()
	case protoreflect.StringKind:
		raw = v.String()
	case protoreflect.BytesKind:
		raw = v.Bytes()
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if isDurationField(field) {
			raw = v.Message().Interface().(*durationpb.Duration).AsDuration()
			break
		}
		raw = v.Message().Interface()
	default:
		return zero, fmt.Errorf("unsupported kind %s", field.Kind())
	}
	value, ok := raw.(V)
	if !ok {
		return zero, fmt.Errorf("expected %s, got %T", reflect.TypeFor[V](), raw)
	}
	return value, nil
}
