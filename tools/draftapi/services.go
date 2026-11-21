package draftapi

import (
	"cmp"
	"go/ast"
	"go/constant"
	"go/types"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/types/descriptorpb"
)

func (c *catalog) addServices(fd *descriptorpb.FileDescriptorProto) {
	base := strings.TrimSuffix(filepath.Base(fd.GetName()), ".proto")
	grpcFile := c.generatedServiceFile(base + "_grpc.pb.go")
	if grpcFile == nil {
		return
	}
	gatewayFile := c.generatedServiceFile(base + ".pb.gw.go")
	for _, service := range fd.Service {
		serviceFeature := c.feature(service.Options)
		goService := c.serviceName(grpcFile, fd.GetPackage()+"."+service.GetName())
		if goService == "" {
			continue
		}
		c.addServiceSymbols(grpcFile, gatewayFile, goService, serviceFeature)
		for _, method := range service.Method {
			feature := cmp.Or(serviceFeature, c.feature(method.Options))
			if feature == "" {
				continue
			}
			fullName := "/" + fd.GetPackage() + "." + service.GetName() + "/" + method.GetName()
			for _, name := range c.pass.Pkg.Scope().Names() {
				obj, ok := c.pass.Pkg.Scope().Lookup(name).(*types.Const)
				if !ok || !c.declaredIn(obj, grpcFile) || obj.Val().Kind() != constant.String || constant.StringVal(obj.Val()) != fullName || !strings.HasSuffix(name, "_FullMethodName") {
					continue
				}
				c.mark(obj, feature)
				goMethod, ok := strings.CutPrefix(strings.TrimSuffix(name, "_FullMethodName"), goService+"_")
				if ok {
					c.addServiceMethod(grpcFile, goService, goMethod, feature, method.GetClientStreaming() || method.GetServerStreaming())
				}
			}
		}
	}
}

func (c *catalog) generatedServiceFile(name string) *ast.File {
	for _, file := range c.pass.Files {
		if filepath.Base(c.pass.Fset.Position(file.Pos()).Filename) == name && ast.IsGenerated(file) {
			return file
		}
	}
	return nil
}

func (c *catalog) declaredIn(obj types.Object, file *ast.File) bool {
	return obj != nil && file != nil && c.pass.Fset.File(obj.Pos()) == c.pass.Fset.File(file.Pos())
}

func (c *catalog) serviceName(file *ast.File, fullName string) string {
	for _, value := range valueSpecs(file) {
		if !strings.HasSuffix(value.Names[0].Name, "_ServiceDesc") {
			continue
		}
		if c.serviceDescriptorName(value.Values[0]) == fullName {
			return strings.TrimSuffix(value.Names[0].Name, "_ServiceDesc")
		}
	}
	return ""
}

func (c *catalog) serviceDescriptorName(expr ast.Expr) string {
	literal, ok := expr.(*ast.CompositeLit)
	if !ok {
		return ""
	}
	for _, elt := range literal.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "ServiceName" {
			continue
		}
		name := c.pass.TypesInfo.Types[kv.Value].Value
		if name != nil && name.Kind() == constant.String {
			return constant.StringVal(name)
		}
	}
	return ""
}

func (c *catalog) addServiceSymbols(grpcFile, gatewayFile *ast.File, service string, feature string) {
	if feature == "" {
		return
	}
	for _, name := range []string{service + "Client", service + "Server", "Unimplemented" + service + "Server", "Unsafe" + service + "Server",
		"New" + service + "Client", "Register" + service + "Server", service + "_ServiceDesc"} {
		c.markServiceSymbol(grpcFile, name, feature)
	}
	for _, suffix := range []string{"Handler", "HandlerClient", "HandlerFromEndpoint", "HandlerServer"} {
		c.markServiceSymbol(gatewayFile, "Register"+service+suffix, feature)
	}
}

func (c *catalog) markServiceSymbol(file *ast.File, name, feature string) {
	if obj := c.pass.Pkg.Scope().Lookup(name); c.declaredIn(obj, file) {
		c.mark(obj, feature)
	}
}

func (c *catalog) addServiceMethod(grpcFile *ast.File, service, method, feature string, streaming bool) {
	scope := c.pass.Pkg.Scope()
	if streaming {
		c.markServiceSymbol(grpcFile, service+"_"+method+"Client", feature)
		c.markServiceSymbol(grpcFile, service+"_"+method+"Server", feature)
	}
	for _, name := range []string{service + "Client", service + "Server", "Unimplemented" + service + "Server", "Unsafe" + service + "Server"} {
		obj, ok := scope.Lookup(name).(*types.TypeName)
		if !ok || !c.declaredIn(obj, grpcFile) {
			continue
		}
		if fn, _, _ := types.LookupFieldOrMethod(types.NewPointer(obj.Type()), true, c.pass.Pkg, method); fn != nil {
			c.mark(fn, feature)
		}
		if iface, ok := obj.Type().Underlying().(*types.Interface); ok {
			for method0 := range iface.Methods() {
				if method0.Name() == method {
					c.mark(method0, feature)
				}
			}
		}
	}
}
