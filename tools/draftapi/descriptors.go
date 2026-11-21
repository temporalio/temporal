package draftapi

import (
	"cmp"
	"errors"
	"fmt"
	"go/ast"
	"go/constant"
	"go/types"
	"reflect"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

type option struct {
	Extendee string
	Number   int32
	// File is the schema file declaring the option.
	File string
}

// draftOptionExtendees is the generators' option table. Discovery requires the exact
// name and extendee; other extensions in the schema package are not draft options.
var draftOptionExtendees = map[string]string{
	"draft": "FieldOptions", "draft_message": "MessageOptions",
	"draft_enum": "EnumOptions", "draft_enum_value": "EnumValueOptions",
	"draft_service": "ServiceOptions", "draft_method": "MethodOptions",
	"draft_oneof": "OneofOptions",
}

type optionsFact struct {
	Options []option
}

func (*optionsFact) AFact() {}

type catalog struct {
	pass    *analysis.Pass
	options []option
	facts   map[types.Object]string
	seen    map[proto.Message]bool
	symbols map[types.Object]string
}

// exportAPIs validates the package's draft declarations and exports facts for its draft
// APIs. Declaration and Go-mapping errors are independent, so both are recorded.
func exportAPIs(pass *analysis.Pass, imported []option, metadata *optionsFact) []string {
	var validationErrors []string
	fail := func(err error) {
		validationErrors = append(validationErrors, fmt.Sprintf("%s: %v", pass.Pkg.Path(), err))
	}
	files, err := readDescriptors(pass)
	if err == nil {
		metadata.Options, err = draftOptions(files)
	}
	if err != nil {
		fail(err)
		return validationErrors
	}
	c := &catalog{
		pass: pass, options: append(imported, metadata.Options...), facts: make(map[types.Object]string),
		seen: make(map[proto.Message]bool), symbols: make(map[types.Object]string),
	}
	for _, err := range []error{c.checkDeclarations(files), c.addAPIs(files)} {
		if err != nil {
			fail(err)
		}
	}
	return validationErrors
}

func (c *catalog) addAPIs(files map[string]*descriptorpb.FileDescriptorProto) error {
	for _, file := range c.pass.Files {
		if generatedProto(c.pass, file) {
			if err := c.addTypes(file, files); err != nil {
				return err
			}
		}
	}
	c.addGetters()
	for _, fd := range files {
		if err := c.checkMappings(fd); err != nil {
			return err
		}
		c.addServices(fd)
	}
	for obj, feature := range c.facts {
		symbol := c.symbols[obj]
		if symbol == "" {
			symbol = objectName(obj)
		}
		c.pass.ExportObjectFact(obj, &apiFact{Feature: feature, Symbol: symbol})
	}
	return nil
}

func valueSpecs(file *ast.File) []*ast.ValueSpec {
	var values []*ast.ValueSpec
	for _, decl := range file.Decls {
		if gen, ok := decl.(*ast.GenDecl); ok {
			for _, spec := range gen.Specs {
				if value, ok := spec.(*ast.ValueSpec); ok && len(value.Names) == 1 && len(value.Values) == 1 {
					values = append(values, value)
				}
			}
		}
	}
	return values
}

func readDescriptors(pass *analysis.Pass) (map[string]*descriptorpb.FileDescriptorProto, error) {
	files := make(map[string]*descriptorpb.FileDescriptorProto)
	for _, file := range pass.Files {
		if !generatedProto(pass, file) {
			continue
		}
		for _, value := range valueSpecs(file) {
			if !strings.HasSuffix(value.Names[0].Name, "_rawDesc") {
				continue
			}
			data, err := descriptorBytes(pass, value.Values[0])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", value.Names[0].Name, err)
			}
			fd := new(descriptorpb.FileDescriptorProto)
			if err := proto.Unmarshal(data, fd); err != nil {
				return nil, fmt.Errorf("decode %s: %w", value.Names[0].Name, err)
			}
			files[value.Names[0].Name] = fd
		}
	}
	return files, nil
}

func draftOptions(files map[string]*descriptorpb.FileDescriptorProto) ([]option, error) {
	var options []option
	for _, fd := range files {
		if fd.GetPackage() != "temporal.api.protometa.v1" {
			continue
		}
		for _, ext := range fd.Extension {
			extendee, ok := draftOptionExtendees[ext.GetName()]
			if !ok {
				continue
			}
			extendee = "google.protobuf." + extendee
			if ext.GetType() != descriptorpb.FieldDescriptorProto_TYPE_STRING {
				return nil, fmt.Errorf("draft option %s must have string type", ext.GetName())
			}
			if strings.TrimPrefix(ext.GetExtendee(), ".") != extendee || ext.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
				return nil, fmt.Errorf("draft option %s must be a singular string extending %s", ext.GetName(), extendee)
			}
			options = append(options, option{extendee, ext.GetNumber(), fd.GetName()})
		}
	}
	return options, nil
}

func (c *catalog) addTypes(file *ast.File, files map[string]*descriptorpb.FileDescriptorProto) error {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil || (fn.Name.Name != "Descriptor" && fn.Name.Name != "EnumDescriptor") {
			continue
		}
		fd, indices := descriptorPath(fn, files)
		if fd == nil {
			continue
		}
		method, ok := c.pass.TypesInfo.Defs[fn.Name].(*types.Func)
		if !ok {
			return fmt.Errorf("invalid descriptor accessor %s", fn.Name.Name)
		}
		sig, ok := method.Type().(*types.Signature)
		if !ok || sig.Recv() == nil {
			return fmt.Errorf("invalid descriptor accessor signature %s", fn.Name.Name)
		}
		typ := namedType(sig.Recv().Type())
		if typ == nil {
			return fmt.Errorf("invalid descriptor accessor receiver %s", fn.Name.Name)
		}
		if err := c.addType(file, fd, indices, typ, fn.Name.Name == "EnumDescriptor"); err != nil {
			return err
		}
	}
	return nil
}

func descriptorBytes(pass *analysis.Pass, expr ast.Expr) ([]byte, error) {
	if value := pass.TypesInfo.Types[expr].Value; value != nil && value.Kind() == constant.String {
		return []byte(constant.StringVal(value)), nil
	}
	if literal, ok := expr.(*ast.CompositeLit); ok {
		data := make([]byte, len(literal.Elts))
		for i, elt := range literal.Elts {
			value := pass.TypesInfo.Types[elt].Value
			if value == nil {
				return nil, errors.New("nonconstant descriptor byte")
			}
			n, ok := constant.Uint64Val(value)
			if !ok || n > 255 {
				return nil, errors.New("invalid descriptor byte")
			}
			data[i] = byte(n)
		}
		return data, nil
	}
	return nil, errors.New("unsupported generated descriptor representation")
}

func descriptorPath(fn *ast.FuncDecl, files map[string]*descriptorpb.FileDescriptorProto) (*descriptorpb.FileDescriptorProto, []int) {
	if len(fn.Body.List) != 1 {
		return nil, nil
	}
	ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 2 {
		return nil, nil
	}
	call, ok := ret.Results[0].(*ast.CallExpr)
	if !ok {
		return nil, nil
	}
	id, ok := call.Fun.(*ast.Ident)
	if !ok {
		return nil, nil
	}
	fd := files[strings.TrimSuffix(id.Name, "GZIP")]
	literal, ok := ret.Results[1].(*ast.CompositeLit)
	if fd == nil || !ok || len(literal.Elts) == 0 {
		return nil, nil
	}
	var indices []int
	for _, expr := range literal.Elts {
		lit, ok := expr.(*ast.BasicLit)
		if !ok {
			return nil, nil
		}
		n, err := strconv.Atoi(lit.Value)
		if err != nil || n < 0 {
			return nil, nil
		}
		indices = append(indices, n)
	}
	return fd, indices
}

func namedType(typ types.Type) *types.Named {
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = ptr.Elem()
	}
	named, ok := types.Unalias(typ).(*types.Named)
	if !ok {
		return nil
	}
	return named
}

func (c *catalog) feature(opts proto.Message) string {
	values := c.draftValues(opts)
	if len(values) != 1 {
		return ""
	}
	return values[0]
}

func (c *catalog) draftValues(opts proto.Message) []string {
	if opts == nil || !opts.ProtoReflect().IsValid() {
		return nil
	}
	// Options are registered extensions when the schema is linked and unknown fields otherwise.
	// Marshal can fail only on invalid UTF-8 in a registered string extension, which the
	// descriptor itself could not have carried; analysis runs decode options as unknown fields.
	data, _ := proto.MarshalOptions{AllowPartial: true}.Marshal(opts)
	extendee := string(opts.ProtoReflect().Descriptor().FullName())
	var features []string
	for len(data) > 0 {
		number, kind, n := protowire.ConsumeTag(data)
		if n < 0 {
			break
		}
		data = data[n:]
		length := protowire.ConsumeFieldValue(number, kind, data)
		if length < 0 {
			break
		}
		for _, opt := range c.options {
			if opt.Number == int32(number) && opt.Extendee == extendee && kind == protowire.BytesType {
				feature, _ := protowire.ConsumeString(data)
				features = append(features, feature)
			}
		}
		data = data[length:]
	}
	return features
}

func (c *catalog) mark(obj types.Object, feature string) {
	if obj != nil && obj.Pkg() == c.pass.Pkg && feature != "" {
		c.facts[obj] = feature
	}
}

func (c *catalog) markType(typ *types.Named, feature string) {
	c.mark(typ.Obj(), feature)
	for method := range typ.Methods() {
		c.mark(method, feature)
	}
}

func (c *catalog) addType(file *ast.File, fd *descriptorpb.FileDescriptorProto, path []int, typ *types.Named, enum bool) error {
	messages := fd.MessageType
	enums := fd.EnumType
	var feature string
	for i, index := range path {
		if enum && i == len(path)-1 {
			if index >= len(enums) {
				return fmt.Errorf("invalid enum descriptor path for %s", typ.Obj().Name())
			}
			c.addEnum(file, typ, enums[index], feature)
			return nil
		}
		if index >= len(messages) {
			return fmt.Errorf("invalid message descriptor path for %s", typ.Obj().Name())
		}
		message := messages[index]
		feature = cmp.Or(feature, c.feature(message.Options))
		if i == len(path)-1 {
			c.addMessage(typ, message, feature)
		}
		messages, enums = message.NestedType, message.EnumType
	}
	return nil
}

func (c *catalog) addMessage(typ *types.Named, message *descriptorpb.DescriptorProto, feature string) {
	c.seen[message] = true
	c.markType(typ, feature)
	structure, ok := typ.Underlying().(*types.Struct)
	if !ok {
		return
	}
	for i := 0; i < structure.NumFields(); i++ {
		field := structure.Field(i)
		c.symbols[field] = objectName(typ.Obj()) + "." + field.Name()
		tag := reflect.StructTag(structure.Tag(i))
		if oneof := tag.Get("protobuf_oneof"); oneof != "" {
			for _, decl := range message.OneofDecl {
				if decl.GetName() == oneof {
					oneofFeature := cmp.Or(feature, c.feature(decl.Options))
					c.mark(field, oneofFeature)
					c.addOneof(field.Type(), message, oneofFeature)
					break
				}
			}
			continue
		}
		c.mark(field, cmp.Or(feature, c.fieldFeature(tag, message)))
	}
}

func (c *catalog) fieldFeature(tag reflect.StructTag, message *descriptorpb.DescriptorProto) string {
	parts := strings.Split(tag.Get("protobuf"), ",")
	if len(parts) < 2 {
		return ""
	}
	for _, field := range message.Field {
		if strconv.Itoa(int(field.GetNumber())) == parts[1] {
			return c.feature(field.Options)
		}
	}
	return ""
}

func (c *catalog) addOneof(typ types.Type, message *descriptorpb.DescriptorProto, inherited string) {
	iface, ok := typ.Underlying().(*types.Interface)
	if !ok {
		return
	}
	for _, name := range c.pass.Pkg.Scope().Names() {
		obj, ok := c.pass.Pkg.Scope().Lookup(name).(*types.TypeName)
		if !ok || obj.IsAlias() {
			continue
		}
		wrapper := namedType(obj.Type())
		if wrapper == nil || wrapper.Obj().Pkg() != c.pass.Pkg || !types.Implements(types.NewPointer(wrapper), iface) {
			continue
		}
		structure, ok := wrapper.Underlying().(*types.Struct)
		if !ok || structure.NumFields() != 1 {
			continue
		}
		feature := cmp.Or(inherited, c.fieldFeature(reflect.StructTag(structure.Tag(0)), message))
		c.markType(wrapper, feature)
		c.mark(structure.Field(0), feature)
		c.symbols[structure.Field(0)] = objectName(wrapper.Obj()) + "." + structure.Field(0).Name()
	}
}

func (c *catalog) addEnum(file *ast.File, typ *types.Named, enum *descriptorpb.EnumDescriptorProto, inherited string) {
	c.seen[enum] = true
	feature := cmp.Or(inherited, c.feature(enum.Options))
	c.markType(typ, feature)
	index := 0
	ast.Inspect(file, func(node ast.Node) bool {
		id, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		obj, ok := c.pass.TypesInfo.Defs[id].(*types.Const)
		if !ok || !types.Identical(obj.Type(), typ) || index >= len(enum.Value) {
			return true
		}
		c.mark(obj, cmp.Or(feature, c.feature(enum.Value[index].Options)))
		index++
		return true
	})
}

func (c *catalog) checkMappings(fd *descriptorpb.FileDescriptorProto) error {
	for _, enum := range fd.EnumType {
		if err := c.checkEnumMapping(enum, false); err != nil {
			return fmt.Errorf("%s: %w", fd.GetName(), err)
		}
	}
	for _, message := range fd.MessageType {
		if err := c.checkMessageMapping(message, false); err != nil {
			return fmt.Errorf("%s: %w", fd.GetName(), err)
		}
	}
	return nil
}

func (c *catalog) checkEnumMapping(enum *descriptorpb.EnumDescriptorProto, inherited bool) error {
	draft := inherited || c.feature(enum.Options) != ""
	for _, value := range enum.Value {
		draft = draft || c.feature(value.Options) != ""
	}
	if draft && !c.seen[enum] {
		return fmt.Errorf("cannot map draft enum %s to Go; unsupported generated descriptor accessors", enum.GetName())
	}
	return nil
}

func (c *catalog) checkMessageMapping(message *descriptorpb.DescriptorProto, inherited bool) error {
	draft := inherited || c.feature(message.Options) != ""
	needsMapping := draft
	for _, field := range message.Field {
		needsMapping = needsMapping || c.feature(field.Options) != ""
	}
	for _, oneof := range message.OneofDecl {
		needsMapping = needsMapping || c.feature(oneof.Options) != ""
	}
	if needsMapping && !message.GetOptions().GetMapEntry() && !c.seen[message] {
		return fmt.Errorf("cannot map draft message %s to Go; unsupported generated descriptor accessors", message.GetName())
	}
	for _, enum := range message.EnumType {
		if err := c.checkEnumMapping(enum, draft); err != nil {
			return err
		}
	}
	for _, nested := range message.NestedType {
		if err := c.checkMessageMapping(nested, draft); err != nil {
			return err
		}
	}
	return nil
}

// addGetters uses the field and wrapper declarations after they have been classified.
func (c *catalog) addGetters() {
	for _, file := range c.pass.Files {
		if !generatedProto(c.pass, file) {
			continue
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Get") {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if id, ok := node.(*ast.Ident); ok {
					c.mark(c.pass.TypesInfo.Defs[fn.Name], c.facts[c.pass.TypesInfo.Uses[id]])
				}
				return true
			})
		}
	}
}
