package dynamicconfig

import (
	"fmt"
	"reflect"
	"sync"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/softassert"
)

var (
	// These should all run at static init time, but use synchronization just in case.
	sharedStructureWarnings        sync.Map
	logSharedStructureWarningsOnce sync.Once
)

func warnDefaultSharedStructure(key Key, def any) {
	if path := hasSharedStructure(reflect.ValueOf(def), "root"); path != "" {
		sharedStructureWarnings.Store(key, path)
	}
}

func logSharedStructureWarnings(logger log.Logger) {
	logSharedStructureWarningsOnce.Do(func() {
		sharedStructureWarnings.Range(func(key, path any) bool {
			softassert.Fail(logger, fmt.Sprintf("default value for %v contains shared structure at %v", key, path))
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
