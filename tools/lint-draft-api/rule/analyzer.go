package rule

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"unicode"

	"golang.org/x/tools/go/analysis"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	LinterName           = "draft-api"
	allowDirectiveText   = "temporal:allow-draft-api"
	protoDescriptorDir   = "proto"
	draftExtensionNumber = protowire.Number(77001)
)

var descriptorSetPaths = findDescriptorSets

// Analyzer reports use of draft additions to stable API types.
var Analyzer = &analysis.Analyzer{
	Name: "draftapi",
	Doc:  "reports draft fields and enum values added to stable API types",
	Run:  run,
}

type symbol struct {
	description string
	name        string
	draft       string
}

type registry struct {
	symbols map[string]symbol
}

type allowDirectiveComment struct {
	comment *ast.Comment
}

func run(pass *analysis.Pass) (any, error) {
	reg, err := loadRegistry()
	if err != nil {
		return nil, err
	}

	for _, file := range pass.Files {
		if isGenerated(file) {
			continue
		}
		allowDirectives := collectAllowDirectives(file)
		usedAllowDirectives := make(map[*ast.Comment]bool)
		ast.Inspect(file, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CompositeLit:
				checkCompositeLit(pass, file, reg, allowDirectives, usedAllowDirectives, n)
			case *ast.SelectorExpr:
				checkSelector(pass, file, reg, allowDirectives, usedAllowDirectives, n)
			default:
			}
			return true
		})
		reportUnusedAllowDirectives(pass, allowDirectives, usedAllowDirectives)
	}

	return nil, nil
}

func loadRegistry() (registry, error) {
	files, err := loadDescriptorFiles()
	if err != nil {
		return registry{}, err
	}
	return buildRegistry(files), nil
}

func buildRegistry(files *protoregistry.Files) registry {
	reg := registry{
		symbols: make(map[string]symbol),
	}
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if !strings.HasPrefix(fd.Path(), "temporal/api/") {
			return true
		}
		goPackage := goPackageForFile(fd)
		for i := 0; i < fd.Messages().Len(); i++ {
			collectMessage(reg, goPackage, "", fd.Messages().Get(i), false)
		}
		for i := 0; i < fd.Enums().Len(); i++ {
			collectEnum(reg, goPackage, fd.Enums().Get(i), false)
		}
		return true
	})
	return reg
}

func loadDescriptorFiles() (*protoregistry.Files, error) {
	descriptorPaths, err := descriptorSetPaths()
	if err != nil {
		return nil, err
	}
	set := &descriptorpb.FileDescriptorSet{}
	for _, descriptorPath := range descriptorPaths {
		data, err := os.ReadFile(descriptorPath)
		if err != nil {
			return nil, err
		}
		nextSet := &descriptorpb.FileDescriptorSet{}
		if err := proto.Unmarshal(data, nextSet); err != nil {
			return nil, fmt.Errorf("read %s: %w", descriptorPath, err)
		}
		set.File = append(set.File, nextSet.File...)
	}
	set.File = dedupeFiles(set.File)
	files, err := protodesc.NewFiles(set)
	if err != nil {
		return nil, fmt.Errorf("read proto descriptor sets: %w", err)
	}
	return files, nil
}

func collectMessage(reg registry, goPackage string, prefix string, md protoreflect.MessageDescriptor, draftParent bool) {
	typeName := string(md.Name())
	if prefix != "" {
		typeName = prefix + "_" + typeName
	}
	isDraft := draftParent || hasDraftOption(md.Options())
	if !isDraft {
		for i := 0; i < md.Fields().Len(); i++ {
			fd := md.Fields().Get(i)
			draft, ok := draftOption(fd.Options())
			if !ok {
				continue
			}
			goField := goCamelCase(string(fd.Name()))
			sym := symbol{
				description: "field " + typeName + "." + goField,
				name:        goField,
				draft:       draft,
			}
			typeKey := goPackage + "." + typeName
			reg.symbols[typeKey+"."+goField] = sym
			reg.symbols[typeKey+".Get"+goField] = sym
		}
	}

	for i := 0; i < md.Messages().Len(); i++ {
		collectMessage(reg, goPackage, typeName, md.Messages().Get(i), isDraft)
	}
	for i := 0; i < md.Enums().Len(); i++ {
		collectEnum(reg, goPackage, md.Enums().Get(i), isDraft)
	}
}

func collectEnum(reg registry, goPackage string, ed protoreflect.EnumDescriptor, draftParent bool) {
	if draftParent || hasDraftOption(ed.Options()) {
		return
	}
	for i := 0; i < ed.Values().Len(); i++ {
		vd := ed.Values().Get(i)
		draft, ok := draftOption(vd.Options())
		if !ok {
			continue
		}
		name := string(vd.Name())
		reg.symbols[goPackage+"."+name] = symbol{
			description: "enum value " + string(ed.FullName()) + "." + name,
			name:        name,
			draft:       draft,
		}
	}
}

func checkCompositeLit(
	pass *analysis.Pass,
	file *ast.File,
	reg registry,
	allowDirectives []allowDirectiveComment,
	usedAllowDirectives map[*ast.Comment]bool,
	lit *ast.CompositeLit,
) {
	typeKey, ok := typeKey(pass.TypesInfo.TypeOf(lit))
	if !ok {
		return
	}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		if sym, ok := reg.symbols[typeKey+"."+key.Name]; ok {
			report(pass, file, allowDirectives, usedAllowDirectives, key.Pos(), sym, false)
		}
	}
}

func checkSelector(
	pass *analysis.Pass,
	file *ast.File,
	reg registry,
	allowDirectives []allowDirectiveComment,
	usedAllowDirectives map[*ast.Comment]bool,
	sel *ast.SelectorExpr,
) {
	if selection := pass.TypesInfo.Selections[sel]; selection != nil {
		if recvKey, ok := typeKey(selection.Recv()); ok {
			if selection.Kind() == types.FieldVal || selection.Kind() == types.MethodVal || selection.Kind() == types.MethodExpr {
				if sym, ok := reg.symbols[recvKey+"."+selection.Obj().Name()]; ok {
					report(pass, file, allowDirectives, usedAllowDirectives, sel.Sel.Pos(), sym, true)
				}
			}
		}
		return
	}

	obj, ok := pass.TypesInfo.Uses[sel.Sel].(*types.Const)
	if !ok || obj.Pkg() == nil {
		return
	}
	if sym, ok := reg.symbols[obj.Pkg().Path()+"."+obj.Name()]; ok {
		report(pass, file, allowDirectives, usedAllowDirectives, sel.Sel.Pos(), sym, true)
	}
}

func report(
	pass *analysis.Pass,
	file *ast.File,
	allowDirectives []allowDirectiveComment,
	usedAllowDirectives map[*ast.Comment]bool,
	pos token.Pos,
	sym symbol,
	allowOverride bool,
) {
	if allowOverride && commentAllows(pass, allowDirectives, usedAllowDirectives, pos, sym.draft) {
		return
	}

	if !allowOverride {
		pass.Reportf(
			pos,
			"draft API %s (%q) cannot be set in a struct literal",
			sym.description,
			sym.draft,
		)
		return
	}

	pass.Reportf(
		pos,
		"draft API %s (%q) requires // %s %s",
		sym.description,
		sym.draft,
		allowDirectiveText,
		sym.draft,
	)
}

func isGenerated(file *ast.File) bool {
	for _, group := range file.Comments {
		if group.Pos() > file.Package {
			break
		}
		for _, comment := range group.List {
			if strings.Contains(comment.Text, "Code generated") && strings.Contains(comment.Text, "DO NOT EDIT") {
				return true
			}
		}
	}
	return false
}

func collectAllowDirectives(file *ast.File) []allowDirectiveComment {
	var directives []allowDirectiveComment
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if strings.Contains(comment.Text, allowDirectiveText) {
				directives = append(directives, allowDirectiveComment{comment: comment})
			}
		}
	}
	return directives
}

func commentAllows(
	pass *analysis.Pass,
	allowDirectives []allowDirectiveComment,
	usedAllowDirectives map[*ast.Comment]bool,
	pos token.Pos,
	draft string,
) bool {
	line := pass.Fset.Position(pos).Line
	for _, directive := range allowDirectives {
		commentLine := pass.Fset.Position(directive.comment.Slash).Line
		if commentLine != line && commentLine != line-1 {
			continue
		}
		if allowsDraft(directive.comment.Text, draft) {
			usedAllowDirectives[directive.comment] = true
			return true
		}
	}
	return false
}

func allowsDraft(text string, draft string) bool {
	_, tail, ok := strings.Cut(text, allowDirectiveText)
	if !ok {
		return false
	}
	fields := strings.Fields(tail)
	return slices.Contains(fields, draft)
}

func reportUnusedAllowDirectives(
	pass *analysis.Pass,
	allowDirectives []allowDirectiveComment,
	usedAllowDirectives map[*ast.Comment]bool,
) {
	for _, directive := range allowDirectives {
		if usedAllowDirectives[directive.comment] {
			continue
		}
		pass.Reportf(
			directive.comment.Slash,
			"unused draft API allow directive",
		)
	}
}

func typeKey(t types.Type) (string, bool) {
	named, ok := namedType(t)
	if !ok || named.Obj().Pkg() == nil {
		return "", false
	}
	return named.Obj().Pkg().Path() + "." + named.Obj().Name(), true
}

func namedType(t types.Type) (*types.Named, bool) {
	for {
		if ptr, ok := t.(*types.Pointer); ok {
			t = ptr.Elem()
			continue
		}
		break
	}
	named, ok := t.(*types.Named)
	return named, ok
}

func findDescriptorSets() ([]string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	for {
		candidate := filepath.Join(wd, protoDescriptorDir)
		descriptorPaths, err := protoDescriptorPaths(candidate)
		if err != nil {
			return nil, err
		}
		if len(descriptorPaths) > 0 {
			return descriptorPaths, nil
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			return nil, fmt.Errorf("could not find proto descriptor sets under %s; run make binpb or make lint from the repository root", protoDescriptorDir)
		}
		wd = parent
	}
}

func protoDescriptorPaths(dir string) ([]string, error) {
	var descriptorPaths []string
	for _, pattern := range []string{"*.bin", "*.binpb"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return nil, err
		}
		descriptorPaths = append(descriptorPaths, matches...)
	}
	return descriptorPaths, nil
}

func dedupeFiles(files []*descriptorpb.FileDescriptorProto) []*descriptorpb.FileDescriptorProto {
	seen := make(map[string]struct{}, len(files))
	deduped := files[:0]
	for _, file := range files {
		name := file.GetName()
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		deduped = append(deduped, file)
	}
	return deduped
}

func goPackageForFile(fd protoreflect.FileDescriptor) string {
	options, ok := fd.Options().(*descriptorpb.FileOptions)
	if ok {
		goPackage := options.GetGoPackage()
		if goPackage != "" {
			pkg, _, _ := strings.Cut(goPackage, ";")
			return pkg
		}
	}
	dir := path.Dir(strings.TrimPrefix(fd.Path(), "temporal/api/"))
	if dir == "." {
		return "go.temporal.io/api"
	}
	return "go.temporal.io/api/" + dir
}

func hasDraftOption(options protoreflect.ProtoMessage) bool {
	_, ok := draftOption(options)
	return ok
}

func draftOption(options protoreflect.ProtoMessage) (string, bool) {
	if options == nil {
		return "", false
	}
	unknown := options.ProtoReflect().GetUnknown()
	for len(unknown) > 0 {
		fieldNumber, wireType, tagLen := protowire.ConsumeTag(unknown)
		if tagLen < 0 {
			return "", false
		}
		unknown = unknown[tagLen:]
		if fieldNumber == draftExtensionNumber && wireType == protowire.BytesType {
			value, valueLen := protowire.ConsumeBytes(unknown)
			if valueLen < 0 {
				return "", false
			}
			return string(value), true
		}
		valueLen := protowire.ConsumeFieldValue(fieldNumber, wireType, unknown)
		if valueLen < 0 {
			return "", false
		}
		unknown = unknown[valueLen:]
	}
	return "", false
}

func goCamelCase(name string) string {
	var b strings.Builder
	capitalizeNext := true
	for _, r := range name {
		if r == '_' {
			capitalizeNext = true
			continue
		}
		if capitalizeNext {
			b.WriteRune(unicode.ToUpper(r))
			capitalizeNext = false
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
