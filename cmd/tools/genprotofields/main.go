// Command genprotofields generates typed field handles (see
// go.temporal.io/server/api/protohelpers) for a fixed set of proto messages.
//
// It reflects over each message's generated Go struct to discover the field's
// Go name and type, and reads the `protobuf` struct tags to recover the proto
// name and kind. The emitted handles let downstream tools build, match, and
// validate messages by walking fields exhaustively, and a companion test uses
// protofield.Verify to fail when a proto field is added without regenerating.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/cmd/tools/codegen"
	"google.golang.org/protobuf/proto"
)

// targets lists the messages to generate handles for. Add messages here as more
// RPCs adopt the protofield helpers.
var targets = []proto.Message{
	&workflowservice.StartWorkflowExecutionRequest{},
	&workflowservice.StartWorkflowExecutionResponse{},
	// WorkflowType is a nested message of StartWorkflowExecutionRequest, used to
	// exercise match.Nested.
	&commonpb.WorkflowType{},
	// Memo exercises the map-field code path.
	&commonpb.Memo{},
	// Command exercises the oneof code path (the "attributes" oneof).
	&commandpb.Command{},
}

const (
	outPackage      = "fields"
	matchPackage    = "match"
	validatePackage = "validate"

	protofieldPath = "go.temporal.io/server/api/protohelpers/protofield"
	validationPath = "go.temporal.io/server/api/protohelpers/validation"
)

func main() {
	outFlag := flag.String("out", ".", "output directory for field handles")
	matchOutFlag := flag.String("match-out", "../match", "output directory for matchers")
	validateOutFlag := flag.String("validate-out", "../validate", "output directory for validators")
	flag.Parse()

	var msgs []*messageModel
	imports := newImports()
	imports.add(protofieldPath)
	for _, t := range targets {
		m := buildMessageModel(t, imports)
		msgs = append(msgs, m)
	}

	writeFile(*outFlag, "fields_gen.go", render(outPackage, imports, msgs))
	writeFile(*matchOutFlag, "match_gen.go", renderMatch(matchPackage, msgs))
	writeFile(*validateOutFlag, "validate_gen.go", renderValidate(validatePackage, imports, msgs))
}

func writeFile(dir, name string, src []byte) {
	formatted, err := format.Source(src)
	if err != nil {
		// Emit the unformatted source to aid debugging before failing.
		_ = os.WriteFile(filepath.Join(dir, name+".debug"), src, 0o644)
		codegen.Fatalf("formatting %s: %v", name, err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), formatted, 0o644); err != nil {
		codegen.Fatalf("writing %s: %v", filepath.Join(dir, name), err)
	}
}

// messageModel is the render-ready description of one message's field handles.
type messageModel struct {
	GoName   string // Go type name, e.g. "StartWorkflowExecutionRequest"
	PkgPath  string // Go import path of the message's package
	TypeExpr string // fully-qualified pointer type, e.g. "*workflowservicepb.StartWorkflowExecutionRequest"
	Fields   []*fieldModel
}

// fieldModel is the render-ready description of one field handle.
type fieldModel struct {
	ProtoName   string // proto name, e.g. "workflow_id" (oneof name for oneofs)
	GoName      string // Go struct field name, e.g. "WorkflowId"
	Constructor string // protofield constructor, e.g. "NewScalar"
	HandleType  string // handle field type, e.g. "protofield.Scalar[*T, string]"
	KindExpr    string // protofield.Kind expression, only for scalars/enums
	IsSetExpr   string // boolean expression over the receiver m
	ValueExpr   string // Go type of the value/element (for get/set closures)
	IsOneof     bool
}

var versionSegment = regexp.MustCompile(`^v\d+$`)

// protoMessageType distinguishes a pointer-to-message field from a proto3
// optional scalar (a pointer to a plain scalar, which does not implement it).
var protoMessageType = reflect.TypeOf((*proto.Message)(nil)).Elem()

func buildMessageModel(msg proto.Message, imports *importSet) *messageModel {
	ptrType := reflect.TypeOf(msg)
	structType := ptrType.Elem()
	typeExpr := "*" + imports.qualify(ptrType.Elem())

	mm := &messageModel{
		GoName:   structType.Name(),
		PkgPath:  structType.PkgPath(),
		TypeExpr: typeExpr,
	}

	for i := 0; i < structType.NumField(); i++ {
		sf := structType.Field(i)
		fm := buildFieldModel(sf, typeExpr, imports)
		if fm == nil {
			continue // not a proto field (state, unknownFields, sizeCache)
		}
		mm.Fields = append(mm.Fields, fm)
	}
	return mm
}

func buildFieldModel(sf reflect.StructField, msgType string, imports *importSet) *fieldModel {
	if oneofName := sf.Tag.Get("protobuf_oneof"); oneofName != "" {
		return &fieldModel{
			ProtoName:   oneofName,
			GoName:      sf.Name,
			Constructor: "NewOneof",
			HandleType:  fmt.Sprintf("protofield.Oneof[%s, any]", msgType),
			IsSetExpr:   fmt.Sprintf("m.%s != nil", sf.Name),
			ValueExpr:   "any",
			IsOneof:     true,
		}
	}

	tag := sf.Tag.Get("protobuf")
	if tag == "" {
		return nil // not a proto field
	}
	protoName := protoNameFromTag(tag)
	if protoName == "" {
		codegen.Fatalf("field %s has no name= in protobuf tag %q", sf.Name, tag)
	}
	isEnum := strings.Contains(tag, ",enum=")

	switch {
	case sf.Type.Kind() == reflect.Map:
		keyExpr := imports.qualify(sf.Type.Key())
		valExpr := imports.qualify(sf.Type.Elem())
		return &fieldModel{
			ProtoName:   protoName,
			GoName:      sf.Name,
			Constructor: "NewMap",
			HandleType:  fmt.Sprintf("protofield.Map[%s, %s, %s]", msgType, keyExpr, valExpr),
			IsSetExpr:   fmt.Sprintf("len(m.%s) > 0", sf.Name),
			ValueExpr:   fmt.Sprintf("map[%s]%s", keyExpr, valExpr),
		}
	case sf.Type.Kind() == reflect.Slice && sf.Type.Elem().Kind() != reflect.Uint8:
		elemExpr := imports.qualify(sf.Type.Elem())
		return &fieldModel{
			ProtoName:   protoName,
			GoName:      sf.Name,
			Constructor: "NewRepeated",
			HandleType:  fmt.Sprintf("protofield.Repeated[%s, %s]", msgType, elemExpr),
			IsSetExpr:   fmt.Sprintf("len(m.%s) > 0", sf.Name),
			ValueExpr:   "[]" + elemExpr,
		}
	case sf.Type.Kind() == reflect.Ptr && sf.Type.Implements(protoMessageType):
		valExpr := imports.qualify(sf.Type)
		return &fieldModel{
			ProtoName:   protoName,
			GoName:      sf.Name,
			Constructor: "NewMessage",
			HandleType:  fmt.Sprintf("protofield.Message[%s, %s]", msgType, valExpr),
			IsSetExpr:   fmt.Sprintf("m.%s != nil", sf.Name),
			ValueExpr:   valExpr,
		}
	case sf.Type.Kind() == reflect.Ptr:
		// proto3 optional scalar (e.g. *int32): a scalar with explicit presence,
		// set exactly when the pointer is non-nil.
		valExpr := imports.qualify(sf.Type)
		kindExpr := "protofield.KindScalar"
		if isEnum {
			kindExpr = "protofield.KindEnum"
		}
		return &fieldModel{
			ProtoName:   protoName,
			GoName:      sf.Name,
			Constructor: "NewScalar",
			HandleType:  fmt.Sprintf("protofield.Scalar[%s, %s]", msgType, valExpr),
			KindExpr:    kindExpr,
			IsSetExpr:   fmt.Sprintf("m.%s != nil", sf.Name),
			ValueExpr:   valExpr,
		}
	default:
		// Singular scalar or enum.
		valExpr := imports.qualify(sf.Type)
		kindExpr := "protofield.KindScalar"
		if isEnum {
			kindExpr = "protofield.KindEnum"
		}
		return &fieldModel{
			ProtoName:   protoName,
			GoName:      sf.Name,
			Constructor: "NewScalar",
			HandleType:  fmt.Sprintf("protofield.Scalar[%s, %s]", msgType, valExpr),
			KindExpr:    kindExpr,
			IsSetExpr:   scalarIsSet(sf.Name, sf.Type),
			ValueExpr:   valExpr,
		}
	}
}

// scalarIsSet returns the zero-check expression for a singular scalar field.
func scalarIsSet(goName string, t reflect.Type) string {
	switch t.Kind() {
	case reflect.String:
		return fmt.Sprintf("m.%s != \"\"", goName)
	case reflect.Bool:
		return fmt.Sprintf("m.%s", goName)
	case reflect.Slice: // []byte
		return fmt.Sprintf("len(m.%s) > 0", goName)
	default: // numeric, enum
		return fmt.Sprintf("m.%s != 0", goName)
	}
}

func protoNameFromTag(tag string) string {
	for _, part := range strings.Split(tag, ",") {
		if strings.HasPrefix(part, "name=") {
			return strings.TrimPrefix(part, "name=")
		}
	}
	return ""
}

// importSet tracks the packages referenced by generated code and assigns a
// stable alias to each.
type importSet struct {
	aliasByPath map[string]string
	pathByAlias map[string]string
}

func newImports() *importSet {
	return &importSet{
		aliasByPath: map[string]string{},
		pathByAlias: map[string]string{},
	}
}

func (s *importSet) add(path string) string {
	if a, ok := s.aliasByPath[path]; ok {
		return a
	}
	base := aliasFor(path)
	alias := base
	for i := 2; ; i++ {
		if _, taken := s.pathByAlias[alias]; !taken {
			break
		}
		alias = fmt.Sprintf("%s%d", base, i)
	}
	s.aliasByPath[path] = alias
	s.pathByAlias[alias] = path
	return alias
}

// qualify returns the Go source expression for t (e.g. "*commonpb.WorkflowType"),
// registering any package it references.
func (s *importSet) qualify(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Ptr:
		return "*" + s.qualify(t.Elem())
	case reflect.Slice:
		return "[]" + s.qualify(t.Elem())
	case reflect.Map:
		return fmt.Sprintf("map[%s]%s", s.qualify(t.Key()), s.qualify(t.Elem()))
	}
	if t.PkgPath() == "" {
		return t.Kind().String() // builtin: string, bool, int32, ...
	}
	alias := s.add(t.PkgPath())
	return alias + "." + t.Name()
}

func (s *importSet) sorted() []struct{ Alias, Path string } {
	var out []struct{ Alias, Path string }
	for path, alias := range s.aliasByPath {
		out = append(out, struct{ Alias, Path string }{alias, path})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// aliasFor derives a package alias from an import path: ".../<pkg>/vN" becomes
// "<pkg>pb"; otherwise the last path segment is used verbatim.
func aliasFor(path string) string {
	segs := strings.Split(path, "/")
	last := segs[len(segs)-1]
	if versionSegment.MatchString(last) && len(segs) >= 2 {
		return segs[len(segs)-2] + "pb"
	}
	return last
}

func render(pkg string, imports *importSet, msgs []*messageModel) []byte {
	var b bytes.Buffer
	fmt.Fprintln(&b, "// Code generated by genprotofields. DO NOT EDIT.")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "package %s\n\n", pkg)
	fmt.Fprintln(&b, "import (")
	for _, imp := range imports.sorted() {
		fmt.Fprintf(&b, "\t%s %q\n", imp.Alias, imp.Path)
	}
	fmt.Fprintln(&b, ")")

	for _, m := range msgs {
		renderMessage(&b, m)
	}
	return b.Bytes()
}

func renderMessage(b *bytes.Buffer, m *messageModel) {
	structName := m.GoName + "FieldsT"
	fmt.Fprintf(b, "\n// %s holds a typed handle for every field of %s.\n", structName, m.TypeExpr)
	fmt.Fprintf(b, "type %s struct {\n", structName)
	for _, f := range m.Fields {
		fmt.Fprintf(b, "\t%s %s\n", f.GoName, f.HandleType)
	}
	fmt.Fprintln(b, "}")

	fmt.Fprintf(b, "\n// %s exposes the field handles of %s.\n", m.GoName, m.TypeExpr)
	fmt.Fprintf(b, "var %s = %s{\n", m.GoName, structName)
	for _, f := range m.Fields {
		renderConstructor(b, m, f)
	}
	fmt.Fprintln(b, "}")

	// All enumerates every field handle for exhaustive iteration.
	fmt.Fprintf(b, "\n// All returns every field handle of %s, for exhaustive iteration.\n", m.TypeExpr)
	fmt.Fprintf(b, "func (f %s) All() []protofield.Field[%s] {\n", structName, m.TypeExpr)
	fmt.Fprintf(b, "\treturn []protofield.Field[%s]{\n", m.TypeExpr)
	for _, f := range m.Fields {
		fmt.Fprintf(b, "\t\tf.%s,\n", f.GoName)
	}
	fmt.Fprintln(b, "\t}")
	fmt.Fprintln(b, "}")
}

// renderMatch emits the match package: a matcher struct per message (one `any`
// field per proto field) plus Equal (exhaustive) and EqualPartial (only-specified)
// assertions, sharing the field model used for the handles.
func renderMatch(pkg string, msgs []*messageModel) []byte {
	imports := newImports()
	const requirePath = "github.com/stretchr/testify/require"
	imports.add(requirePath)
	for _, m := range msgs {
		imports.add(m.PkgPath)
	}

	var b bytes.Buffer
	fmt.Fprintln(&b, "// Code generated by genprotofields. DO NOT EDIT.")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "package %s\n\n", pkg)
	fmt.Fprintln(&b, "import (")
	for _, imp := range imports.sorted() {
		fmt.Fprintf(&b, "\t%s %q\n", imp.Alias, imp.Path)
	}
	fmt.Fprintln(&b, ")")

	for _, m := range msgs {
		typeExpr := "*" + imports.aliasByPath[m.PkgPath] + "." + m.GoName

		fmt.Fprintf(&b, "\n// %s matches %s field by field. Assign each field a\n", m.GoName, typeExpr)
		fmt.Fprintln(&b, "// match.Matcher or a bare literal (compared with Eq).")
		fmt.Fprintf(&b, "type %s struct {\n", m.GoName)
		for _, f := range m.Fields {
			fmt.Fprintf(&b, "\t%s any\n", f.GoName)
		}
		fmt.Fprintln(&b, "}")

		fmt.Fprintf(&b, "\n// Equal asserts that actual matches m exhaustively: every field must be\n")
		fmt.Fprintln(&b, "// assigned a matcher or literal (use match.Any() to ignore one).")
		fmt.Fprintf(&b, "func (m %s) Equal(t require.TestingT, actual %s) {\n", m.GoName, typeExpr)
		fmt.Fprintf(&b, "\treportFailures(t, %q, m.check(actual, true))\n", m.GoName)
		fmt.Fprintln(&b, "}")

		fmt.Fprintf(&b, "\n// EqualPartial asserts that actual matches the fields set on m; unset\n")
		fmt.Fprintln(&b, "// (nil) fields are ignored.")
		fmt.Fprintf(&b, "func (m %s) EqualPartial(t require.TestingT, actual %s) {\n", m.GoName, typeExpr)
		fmt.Fprintf(&b, "\treportFailures(t, %q, m.check(actual, false))\n", m.GoName)
		fmt.Fprintln(&b, "}")

		// checkAny lets this matcher be nested inside another via match.Nested.
		fmt.Fprintf(&b, "\nfunc (m %s) checkAny(got any, exhaustive bool) []string {\n", m.GoName)
		fmt.Fprintf(&b, "\tactual, ok := got.(%s)\n", typeExpr)
		fmt.Fprintf(&b, "\tif !ok {\n\t\treturn []string{\"value is not a %s\"}\n\t}\n", m.GoName)
		fmt.Fprintln(&b, "\treturn m.check(actual, exhaustive)")
		fmt.Fprintln(&b, "}")

		fmt.Fprintf(&b, "\nfunc (m %s) check(actual %s, exhaustive bool) []string {\n", m.GoName, typeExpr)
		fmt.Fprintf(&b, "\te := newEval(exhaustive)\n")
		for _, f := range m.Fields {
			fmt.Fprintf(&b, "\te.field(%q, m.%s, actual.Get%s())\n", f.ProtoName, f.GoName, f.GoName)
		}
		fmt.Fprintln(&b, "\treturn e.failures")
		fmt.Fprintln(&b, "}")
	}
	return b.Bytes()
}

// renderValidate emits the validate package: a typed FieldValidators struct per
// message (one validation.FieldValidator per proto field) plus ValidateAndNormalize
// and RegisterValidator. Callers fill the struct with config-aware field validators;
// ValidateAndNormalize invokes each in order. It reuses the shared import set
// (dropping the protofield core, adding the validation core) so field value types
// keep the same aliases as the field handles.
func renderValidate(pkg string, imports *importSet, msgs []*messageModel) []byte {
	validationAlias := aliasFor(validationPath)

	var b bytes.Buffer
	fmt.Fprintln(&b, "// Code generated by genprotofields. DO NOT EDIT.")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "package %s\n\n", pkg)
	fmt.Fprintln(&b, "import (")
	fmt.Fprintf(&b, "\t%s %q\n", validationAlias, validationPath)
	for _, imp := range imports.sorted() {
		if imp.Path == protofieldPath {
			continue // validators don't use the field-handle core
		}
		fmt.Fprintf(&b, "\t%s %q\n", imp.Alias, imp.Path)
	}
	fmt.Fprintln(&b, ")")

	for _, m := range msgs {
		ptrType := m.TypeExpr                            // *pkg.Msg
		valueType := strings.TrimPrefix(m.TypeExpr, "*") // pkg.Msg (the FieldValidator type param)

		fmt.Fprintf(&b, "\n// %sFieldValidators validates every field of %s. Fill each\n", m.GoName, ptrType)
		fmt.Fprintln(&b, "// field with a validation.FieldValidator (e.g. via validation.Field); the")
		fmt.Fprintln(&b, "// exhaustive struct forces a decision for every proto field.")
		fmt.Fprintf(&b, "type %sFieldValidators struct {\n", m.GoName)
		for _, f := range m.Fields {
			fmt.Fprintf(&b, "\t%s %s.FieldValidator[%s, %s]\n", f.GoName, validationAlias, valueType, f.ValueExpr)
		}
		fmt.Fprintln(&b, "}")

		fmt.Fprintf(&b, "\n// ValidateAndNormalize runs every field validator on req in order.\n")
		fmt.Fprintf(&b, "func (v %sFieldValidators) ValidateAndNormalize(req %s) error {\n", m.GoName, ptrType)
		for _, f := range m.Fields {
			fmt.Fprintf(&b, "\tif err := v.%s(req, %q, req.Get%s()); err != nil {\n\t\treturn err\n\t}\n", f.GoName, f.ProtoName, f.GoName)
		}
		fmt.Fprintln(&b, "\treturn nil")
		fmt.Fprintln(&b, "}")

		fmt.Fprintf(&b, "\n// RegisterValidator adds v to registry for type-based dispatch.\n")
		fmt.Fprintf(&b, "func (v %sFieldValidators) RegisterValidator(registry *%s.ValidatorRegistry) error {\n", m.GoName, validationAlias)
		fmt.Fprintf(&b, "\treturn %s.RegisterValidator[%s](registry, v)\n", validationAlias, valueType)
		fmt.Fprintln(&b, "}")
	}
	return b.Bytes()
}

func renderConstructor(b *bytes.Buffer, m *messageModel, f *fieldModel) {
	get := fmt.Sprintf("func(m %s) %s { return m.%s }", m.TypeExpr, f.ValueExpr, f.GoName)
	isSet := fmt.Sprintf("func(m %s) bool { return %s }", m.TypeExpr, f.IsSetExpr)

	fmt.Fprintf(b, "\t%s: protofield.%s(\n", f.GoName, f.Constructor)
	fmt.Fprintf(b, "\t\t%q, %q,\n", f.ProtoName, f.GoName)
	if f.KindExpr != "" {
		fmt.Fprintf(b, "\t\t%s,\n", f.KindExpr)
	}
	fmt.Fprintf(b, "\t\t%s,\n", isSet)
	fmt.Fprintf(b, "\t\t%s,\n", get)
	// A oneof handle is read-only: its wrapper type is unexported, so no setter.
	if !f.IsOneof {
		set := fmt.Sprintf("func(m %s, v %s) { m.%s = v }", m.TypeExpr, f.ValueExpr, f.GoName)
		fmt.Fprintf(b, "\t\t%s,\n", set)
	}
	fmt.Fprintln(b, "\t),")
}
