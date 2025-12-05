package dynamicconfig

import (
	"fmt"
	"reflect"
	"sync"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/softassert"
)

var (
	// These should all run at static init time, but use synchronization just in case.
	sharedStructureWarnings        sync.Map
	logSharedStructureWarningsOnce sync.Once
)

func warnDefaultSharedStructure(key string, def any) {
	if path := hasSharedStructure(reflect.ValueOf(def), "root"); path != "" {
		sharedStructureWarnings.Store(key, path)
	}
}

func logSharedStructureWarnings(logger log.Logger) {
	// If you see this warning, it means that a default value used in New*TypedSetting has a
	// non-nil slice or map in it. That can lead to confusing behavior since the value from
	// dynamic config will be merged over the default value (e.g. the slice will be appended
	// to, not replaced). If that behavior is desired, you can avoid this warning by using
	// New*TypedSettingWithConverter and referring to dynamicconfig.ConvertStructure
	// explicitly. Otherwise use nil slices and maps, including at the top level
	// (so `[]string(nil)` instead of `[]string{}`).
	logSharedStructureWarningsOnce.Do(func() {
		sharedStructureWarnings.Range(func(key, path any) bool {
			softassert.Fail(logger,
				"default value contains shared structure",
				tag.Key(fmt.Sprintf("%v", key)),
				tag.Value(path))
			return true
		})
	})
}

func hasSharedStructure(v reflect.Value, path string) string {
	// nolint:exhaustive // deliberately not exhaustive
	switch v.Kind() {
	case reflect.Map, reflect.Slice, reflect.Pointer:
		if !v.IsNil() {
			return path
		}
	case reflect.Interface:
		if !v.IsNil() {
			return hasSharedStructure(v.Elem(), path)
		}
	case reflect.Struct:
		for i := range v.NumField() {
			if p := hasSharedStructure(v.Field(i), path+"."+v.Type().Field(i).Name); p != "" {
				return p
			}
		}
	case reflect.Array:
		for i := range v.Len() {
			if p := hasSharedStructure(v.Index(i), path+"."+fmt.Sprintf("[%d]", i)); p != "" {
				return p
			}
		}
	}
	return ""
}
