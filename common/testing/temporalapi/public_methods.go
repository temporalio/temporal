package temporalapi

import (
	"reflect"
	"regexp"
)

var publicMethodRgx = regexp.MustCompile("^[A-Z]")

// WalkExportedMethods calls the provided callback on each method declared as public on the
// specified object.
// This prevents the `mustEmbedUnimplementedFooBarBaz` method required by the GRPC v2
// gateway from polluting our tests.
func WalkExportedMethods(obj any, cb func(reflect.Method)) {
	v := reflect.ValueOf(obj)
	if !v.IsValid() {
		return
	}

	t := v.Type()
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	for method := range t.Methods() {
		if publicMethodRgx.MatchString(method.Name) {
			cb(method)
		}
	}
}
