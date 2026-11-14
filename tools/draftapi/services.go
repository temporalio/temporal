package draftapi

import (
	"go/ast"
	"go/constant"
	"go/types"
	"slices"
	"strings"

	"google.golang.org/protobuf/types/descriptorpb"
)

func (c *catalog) addServices(fd *descriptorpb.FileDescriptorProto) {
	for _, service := range fd.Service {
		serviceFeatures := c.features(service.Options)
		goService := c.serviceName(fd.GetPackage() + "." + service.GetName())
		if goService == "" {
			continue
		}
		c.addServiceSymbols(goService, serviceFeatures)
		for _, method := range service.Method {
			features := append(slices.Clone(serviceFeatures), c.features(method.Options)...)
			if len(features) == 0 {
				continue
			}
			fullName := "/" + fd.GetPackage() + "." + service.GetName() + "/" + method.GetName()
			for _, name := range c.pass.Pkg.Scope().Names() {
				obj, ok := c.pass.Pkg.Scope().Lookup(name).(*types.Const)
				if !ok || obj.Val().Kind() != constant.String || constant.StringVal(obj.Val()) != fullName || !strings.HasSuffix(name, "_FullMethodName") {
					continue
				}
				c.mark(obj, features)
				goMethod, ok := strings.CutPrefix(strings.TrimSuffix(name, "_FullMethodName"), goService+"_")
				if ok {
					c.addServiceMethod(goService, goMethod, features)
				}
			}
		}
	}
}

func (c *catalog) serviceName(fullName string) string {
	for _, file := range c.pass.Files {
		for _, value := range valueSpecs(file) {
			if !strings.HasSuffix(value.Names[0].Name, "_ServiceDesc") {
				continue
			}
			if c.serviceDescriptorName(value.Values[0]) == fullName {
				return strings.TrimSuffix(value.Names[0].Name, "_ServiceDesc")
			}
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

func (c *catalog) addServiceSymbols(service string, features []string) {
	for _, name := range []string{service + "Client", service + "Server", "Unimplemented" + service + "Server", "Unsafe" + service + "Server",
		"New" + service + "Client", "Register" + service + "Server", service + "_ServiceDesc"} {
		c.mark(c.pass.Pkg.Scope().Lookup(name), features)
	}
}

func (c *catalog) addServiceMethod(service, method string, features []string) {
	scope := c.pass.Pkg.Scope()
	for _, name := range []string{service + "Client", service + "Server", "Unimplemented" + service + "Server", "Unsafe" + service + "Server"} {
		obj, ok := scope.Lookup(name).(*types.TypeName)
		if !ok {
			continue
		}
		if fn, _, _ := types.LookupFieldOrMethod(types.NewPointer(obj.Type()), true, c.pass.Pkg, method); fn != nil {
			c.mark(fn, features)
		}
		if iface, ok := obj.Type().Underlying().(*types.Interface); ok {
			for i := 0; i < iface.NumMethods(); i++ {
				if iface.Method(i).Name() == method {
					c.mark(iface.Method(i), features)
				}
			}
		}
	}
}
