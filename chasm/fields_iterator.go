package chasm

import (
	"iter"
	"reflect"
	"strings"

	"go.temporal.io/api/serviceerror"
)

const (
	chasmFieldTypePrefix      = "chasm.Field["
	chasmCollectionTypePrefix = "chasm.Collection["

	fieldNameTag = "name"
)

type fieldKind uint8

const (
	fieldKindUnspecified fieldKind = iota
	fieldKindData
	fieldKindSubField
	fieldKindSubCollection
)

type fieldInfo struct {
	val  reflect.Value
	typ  reflect.Type
	name string
	kind fieldKind
	err  error
}

//nolint:revive // cognitive complexity 26 (> max enabled 25)
func fieldsOf(valueV reflect.Value) iter.Seq[fieldInfo] {
	valueT := valueV.Type()
	dataFieldName := ""
	return func(yield func(fi fieldInfo) bool) {
		for i := 0; i < valueT.Elem().NumField(); i++ {
			fieldV := valueV.Elem().Field(i)
			fieldT := fieldV.Type()
			if fieldT == UnimplementedComponentT {
				continue
			}
			fieldN := fieldName(valueT.Elem().Field(i))

			var fieldErr error
			fieldK := fieldKindUnspecified
			if fieldT.AssignableTo(protoMessageT) {
				if dataFieldName != "" {
					fieldErr = serviceerror.NewInternalf("%s.%s: only one data field %s (implements proto.Message) allowed in component", valueT, fieldN, dataFieldName)
				}
				dataFieldName = fieldN
				fieldK = fieldKindData
			} else {
				prefix := genericTypePrefix(fieldT)
				if strings.HasPrefix(prefix, "*") {
					fieldErr = serviceerror.NewInternalf("%s.%s: chasm field type %s must not be a pointer", valueT, fieldN, fieldT)
				} else {
					switch prefix {
					case chasmFieldTypePrefix:
						fieldK = fieldKindSubField
					case chasmCollectionTypePrefix:
						fieldK = fieldKindSubCollection
					default:
						fieldErr = serviceerror.NewInternalf("%s.%s: unsupported field type %s: must implement proto.Message, or be chasm.Field[T] or chasm.Collection[T]", valueT, fieldN, fieldT)
					}
				}

			}

			if !yield(fieldInfo{val: fieldV, typ: fieldT, name: fieldN, kind: fieldK, err: fieldErr}) {
				return
			}
		}
		// If the data field is not found, generate one more fake field with only an error set.
		if dataFieldName == "" {
			yield(fieldInfo{err: serviceerror.NewInternalf("%s: no data field (implements proto.Message) found", valueT)})
		}
	}
}

func genericTypePrefix(t reflect.Type) string {
	tn := t.String()
	bracketPos := strings.Index(tn, "[")
	if bracketPos == -1 {
		return ""
	}
	return tn[:bracketPos+1]
}

func fieldName(f reflect.StructField) string {
	if tagName := f.Tag.Get(fieldNameTag); tagName != "" {
		return tagName
	}
	return f.Name
}
