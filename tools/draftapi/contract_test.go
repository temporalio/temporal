package draftapi

import (
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	_ "go.temporal.io/api/errordetails/v1"
	_ "go.temporal.io/api/operatorservice/v1"
	protometapb "go.temporal.io/api/protometa/v1"
	_ "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/draft/testfixture"
	"golang.org/x/mod/modfile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TestAPIContract checks that named Go references agree with the linked descriptors.
// It covers messages, fields, oneofs, enums, services, and RPCs.
func TestAPIContract(t *testing.T) {
	source, expected := apiContractSource(t)

	serverRoot, err := filepath.Abs("../..")
	require.NoError(t, err)
	dir := t.TempDir()
	writeContractModule(t, serverRoot, dir, source)
	tool := filepath.Join(t.TempDir(), "draftapi")
	build := exec.CommandContext(t.Context(), "go", "build", "-tags", "test_dep", "-o", tool, "./cmd/tools/draftapi")
	build.Dir = serverRoot
	output, err := build.CombinedOutput()
	require.NoError(t, err, "%s", output)

	cmd := exec.CommandContext(t.Context(), "go", "vet", "-tags", "test_dep", "-vettool="+tool, "./contract")
	cmd.Dir = dir
	output, err = cmd.CombinedOutput()
	diagnostic := regexp.MustCompile(`contract\.go:(\d+):\d+: use of draft API \S+ \(([^)]*)\)`)
	actual := map[int]string{}
	for _, match := range diagnostic.FindAllStringSubmatch(string(output), -1) {
		line, convErr := strconv.Atoi(match[1])
		require.NoError(t, convErr)
		require.NotContains(t, actual, line, "multiple diagnostics for one reference")
		actual[line] = match[2]
	}
	if err != nil && len(actual) == 0 {
		t.Fatalf("go vet failed without draft diagnostics: %v\n%s", err, output)
	}
	lines := strings.Split(source, "\n")
	for _, line := range slices.Sorted(maps.Keys(maps.Collect(func(yield func(int, bool) bool) {
		for line := range expected {
			yield(line, true)
		}
		for line := range actual {
			yield(line, true)
		}
	}))) {
		if expected[line] != actual[line] {
			t.Errorf("contract.go:%d %s: want draft features %v, got %v", line, strings.TrimSpace(lines[line-1]), expected[line], actual[line])
		}
	}
}

const contractFixtureImport = "go.temporal.io/api/drafttest"

// apiContractSource generates the referencing package and the feature expected per line.
func apiContractSource(t *testing.T) (string, map[int]string) {
	t.Helper()
	aliases := map[string]string{}
	fixturePkg := reflect.TypeFor[testfixture.Message]().PkgPath()
	alias := func(pkgPath string) string {
		if pkgPath == fixturePkg {
			pkgPath = contractFixtureImport
		}
		if aliases[pkgPath] == "" {
			aliases[pkgPath] = fmt.Sprintf("p%d", len(aliases))
		}
		return aliases[pkgPath]
	}
	type entry struct {
		code    string
		feature string
	}
	var body []entry
	add := func(code string, descriptors ...protoreflect.Descriptor) {
		var feature string
		if len(descriptors) > 0 {
			feature = contractFeature(descriptors[0])
		}
		body = append(body, entry{code, feature})
	}
	visitEnums := func(enums protoreflect.EnumDescriptors) {
		for i := 0; i < enums.Len(); i++ {
			enum := enums.Get(i)
			typ, err := protoregistry.GlobalTypes.FindEnumByName(enum.FullName())
			require.NoError(t, err)
			goType := reflect.TypeOf(typ.New(0))
			pkg := alias(goType.PkgPath())
			add(fmt.Sprintf("var _ %s.%s", pkg, goType.Name()), enum)
			var prefix string
			if parent, ok := enum.Parent().(protoreflect.MessageDescriptor); ok {
				prefix = goMessageType(t, parent).Name() + "_"
			} else if goType.PkgPath() == fixturePkg {
				// The API module removes the type prefix from top-level enum values.
				prefix = goType.Name() + "_"
			}
			for j := 0; j < enum.Values().Len(); j++ {
				value := enum.Values().Get(j)
				add(fmt.Sprintf("var _ = %s.%s%s", pkg, prefix, value.Name()), value)
			}
		}
	}
	var visitMessages func(protoreflect.MessageDescriptors)
	visitMessages = func(messages protoreflect.MessageDescriptors) {
		for i := 0; i < messages.Len(); i++ {
			message := messages.Get(i)
			visitMessages(message.Messages())
			visitEnums(message.Enums())
			if message.IsMapEntry() {
				continue
			}
			goType := goMessageType(t, message)
			pkg := alias(goType.PkgPath())
			add("func _() {")
			add(fmt.Sprintf("	var m *%s.%s", pkg, goType.Name()), message)
			add("	_ = m")
			for j := 0; j < message.Oneofs().Len(); j++ {
				oneof := message.Oneofs().Get(j)
				if !oneof.IsSynthetic() {
					add(fmt.Sprintf("	_ = m.Get%s()", generatedOneofField(t, goType, oneof).Name), oneof)
				}
			}
			for j := 0; j < message.Fields().Len(); j++ {
				field := message.Fields().Get(j)
				name := goCamelCase(string(field.Name()))
				getter := "Get" + name
				if _, ok := reflect.PointerTo(goType).MethodByName(getter); !ok {
					t.Fatalf("%s has no getter %s for %s", goType, getter, field.FullName())
				}
				add(fmt.Sprintf("	_ = m.%s()", getter), field)
				if oneof := field.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() {
					add(fmt.Sprintf("	_ = %s.%s{}", pkg, oneofWrapperName(t, goType, field)), field)
				}
			}
			add("}")
		}
	}
	visitServices := func(services protoreflect.ServiceDescriptors) {
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			name := goCamelCase(string(service.Name()))
			file := service.ParentFile()
			pkgPath, _, _ := strings.Cut(file.Options().(*descriptorpb.FileOptions).GetGoPackage(), ";")
			pkg := alias(pkgPath)
			for _, serviceType := range []string{name + "Client", name + "Server", "Unimplemented" + name + "Server", "Unsafe" + name + "Server"} {
				add(fmt.Sprintf("var _ %s.%s", pkg, serviceType), service)
			}
			for _, symbol := range []string{"New" + name + "Client", "Register" + name + "Server", name + "_ServiceDesc"} {
				add(fmt.Sprintf("var _ = %s.%s", pkg, symbol), service)
			}
			add("func _() {")
			add(fmt.Sprintf("	var client %s.%sClient", pkg, name), service)
			add(fmt.Sprintf("	var server %s.%sServer", pkg, name), service)
			add(fmt.Sprintf("	var unimplemented %s.Unimplemented%sServer", pkg, name), service)
			for j := 0; j < service.Methods().Len(); j++ {
				method := service.Methods().Get(j)
				methodName := goCamelCase(string(method.Name()))
				add(fmt.Sprintf("	_ = %s.%s_%s_FullMethodName", pkg, name, methodName), method)
				add(fmt.Sprintf("	_ = client.%s", methodName), method)
				add(fmt.Sprintf("	_ = server.%s", methodName), method)
				add(fmt.Sprintf("	_ = unimplemented.%s", methodName), method)
			}
			add("\t_, _, _ = client, server, unimplemented")
			add("}")
		}
	}
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		if strings.HasPrefix(string(file.Package()), "temporal.api.") {
			visitMessages(file.Messages())
			visitEnums(file.Enums())
			visitServices(file.Services())
		}
		return true
	})
	require.NotEmpty(t, body)

	var b strings.Builder
	b.WriteString("package contract\n\nimport (\n")
	for _, pkgPath := range slices.Sorted(maps.Keys(aliases)) {
		fmt.Fprintf(&b, "\t%s %q\n", aliases[pkgPath], pkgPath)
	}
	b.WriteString(")\n\n")
	line := strings.Count(b.String(), "\n") + 1
	expected := map[int]string{}
	for _, e := range body {
		b.WriteString(e.code + "\n")
		if e.feature != "" {
			expected[line] = e.feature
		}
		line++
	}
	return b.String(), expected
}

// contractFeature reads all permitted annotations and follows declaration containment.
func contractFeature(d protoreflect.Descriptor) string {
	var option protoreflect.ExtensionType
	parent := d.Parent()
	switch d := d.(type) {
	case protoreflect.MessageDescriptor:
		option = protometapb.E_DraftMessage
	case protoreflect.FieldDescriptor:
		option = protometapb.E_Draft
		if oneof := d.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() {
			parent = oneof
		}
	case protoreflect.EnumDescriptor:
		option = protometapb.E_DraftEnum
	case protoreflect.EnumValueDescriptor:
		option = protometapb.E_DraftEnumValue
	case protoreflect.OneofDescriptor:
		option = protometapb.E_DraftOneof
	case protoreflect.ServiceDescriptor:
		option = protometapb.E_DraftService
	case protoreflect.MethodDescriptor:
		option = protometapb.E_DraftMethod
	default:
		return ""
	}
	if feature, _ := proto.GetExtension(d.Options(), option).(string); feature != "" {
		return feature
	}
	return contractFeature(parent)
}

func goMessageType(t *testing.T, message protoreflect.MessageDescriptor) reflect.Type {
	t.Helper()
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(message.FullName())
	require.NoError(t, err)
	return reflect.TypeOf(messageType.Zero().Interface()).Elem()
}

// oneofWrapperName reads the concrete wrapper type assigned by the generated message.
func oneofWrapperName(t *testing.T, goType reflect.Type, field protoreflect.FieldDescriptor) string {
	t.Helper()
	value := reflect.New(goType)
	message := value.Interface().(proto.Message).ProtoReflect()
	message.Set(field, message.NewField(field))
	member := generatedOneofField(t, goType, field.ContainingOneof())
	return value.Elem().FieldByIndex(member.Index).Elem().Type().Elem().Name()
}

func generatedOneofField(t *testing.T, goType reflect.Type, oneof protoreflect.OneofDescriptor) reflect.StructField {
	t.Helper()
	for member := range goType.Fields() {
		if member.Tag.Get("protobuf_oneof") == string(oneof.Name()) {
			return member
		}
	}
	t.Fatalf("no generated oneof for %s", oneof.FullName())
	return reflect.StructField{}
}

// goCamelCase is protoc-gen-go's conversion of a field name to its Go name.
func goCamelCase(s string) string {
	var b []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '.' && i+1 < len(s) && isASCIILower(s[i+1]):
		case c == '.':
			b = append(b, '_')
		case c == '_' && (i == 0 || s[i-1] == '.'):
			b = append(b, 'X')
		case c == '_' && i+1 < len(s) && isASCIILower(s[i+1]):
		case isASCIIDigit(c):
			b = append(b, c)
		default:
			if isASCIILower(c) {
				c -= 'a' - 'A'
			}
			b = append(b, c)
			for ; i+1 < len(s) && isASCIILower(s[i+1]); i++ {
				b = append(b, s[i+1])
			}
		}
	}
	return string(b)
}

func isASCIILower(c byte) bool { return 'a' <= c && c <= 'z' }
func isASCIIDigit(c byte) bool { return '0' <= c && c <= '9' }

// writeContractModule writes a copy of the server module holding only the contract
// package, with the server's dependencies and relative replaces made absolute.
func writeContractModule(t *testing.T, serverRoot, dir, source string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(serverRoot, "go.mod"))
	require.NoError(t, err)
	mod, err := modfile.Parse("go.mod", data, nil)
	require.NoError(t, err)
	var relative []modfile.Replace
	for _, replacement := range mod.Replace {
		if replacement.New.Version == "" && !filepath.IsAbs(replacement.New.Path) {
			relative = append(relative, *replacement)
		}
	}
	for _, replacement := range relative {
		require.NoError(t, mod.DropReplace(replacement.Old.Path, replacement.Old.Version))
		require.NoError(t, mod.AddReplace(replacement.Old.Path, replacement.Old.Version, filepath.Join(serverRoot, replacement.New.Path), ""))
	}
	// Use an API import path so the analyzer inspects the fixture alongside the real API.
	fixtureDir := filepath.Join(dir, "fixture")
	require.NoError(t, os.Mkdir(fixtureDir, 0700))
	for _, name := range []string{"filter.pb.go", "filter_grpc.pb.go"} {
		contents, err := os.ReadFile(filepath.Join(serverRoot, "common/draft/testfixture", name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(fixtureDir, name), contents, 0600))
	}
	fixtureMod := fmt.Sprintf("module %s\n\ngo %s\n", contractFixtureImport, mod.Go.Version)
	require.NoError(t, os.WriteFile(filepath.Join(fixtureDir, "go.mod"), []byte(fixtureMod), 0600))
	require.NoError(t, mod.AddRequire(contractFixtureImport, "v0.0.0"))
	require.NoError(t, mod.AddReplace(contractFixtureImport, "", "./fixture", ""))
	data, err = mod.Format()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), data, 0600))
	data, err = os.ReadFile(filepath.Join(serverRoot, "go.sum"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.sum"), data, 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "contract"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "contract", "contract.go"), []byte(source), 0600))
}
