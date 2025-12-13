package chasm

import (
	"iter"
	"reflect"
	"strings"

	"go.temporal.io/api/serviceerror"
)

const (
	chasmFieldTypePrefix         = "chasm.Field["
	chasmMapTypePrefix           = "chasm.Map["
	chasmMSPointerType           = "chasm.MSPointer"
	chasmParentPointerTypePrefix = "chasm.ParentPtr["

	fieldNameTag = "name"
)

type fieldKind uint8

const (
	fieldKindUnspecified fieldKind = iota
	fieldKindData
	fieldKindSubField
	fieldKindSubMap
	fieldKindMutableState
	fieldKindParentPtr
)

type fieldInfo struct {
	val  reflect.Value
	typ  reflect.Type
	name string
	kind fieldKind
	err  error
}

// fieldsOf iterates across all CHASM-managed fields of a struct. Other fields
// are not yielded.
//
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
					switch prefix[1:] {
					case chasmFieldTypePrefix,
						chasmMapTypePrefix,
						chasmMSPointerType,
						chasmParentPointerTypePrefix:
						fieldErr = serviceerror.NewInternalf("%s.%s: CHASM fields must not be pointers", valueT, fieldN)
					default:
						continue
					}
				} else {
					switch prefix {
					case chasmFieldTypePrefix:
						fieldK = fieldKindSubField
					case chasmMapTypePrefix:
						fieldK = fieldKindSubMap
					case chasmMSPointerType:
						fieldK = fieldKindMutableState
					case chasmParentPointerTypePrefix:
						fieldK = fieldKindParentPtr
					default:
						continue // Skip non-CHASM fields.
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

// unmanagedFieldsOf yields all non-CHASM managed fields of a struct.
func unmanagedFieldsOf(valueT reflect.Type) iter.Seq[fieldInfo] {
	return func(yield func(fi fieldInfo) bool) {
		if valueT.Kind() == reflect.Pointer {
			valueT = valueT.Elem()
		}
		for i := range valueT.NumField() {
			fieldT := valueT.Field(i).Type
			if fieldT == UnimplementedComponentT {
				continue
			}

			// Skip the data field, which is always CHASM-managed.
			if fieldT.AssignableTo(protoMessageT) {
				continue
			}

			fieldN := fieldName(valueT.Field(i))
			prefix := genericTypePrefix(fieldT)
			switch prefix {
			case chasmFieldTypePrefix,
				chasmMapTypePrefix,
				chasmMSPointerType,
				chasmParentPointerTypePrefix:
				continue // Skip CHASM fields.
			default:
				if !yield(fieldInfo{typ: fieldT, name: fieldN}) {
					return
				}
			}
		}
	}
}

func genericTypePrefix(t reflect.Type) string {
	tn := t.String()
	if tn == chasmMSPointerType {
		return chasmMSPointerType
	}
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

// visibilityFieldT is the reflect.Type for Field[*Visibility], used to detect
// components that use Visibility at registration time.
var visibilityFieldT = reflect.TypeFor[Field[*Visibility]]()

// hasVisibilityField returns true if the given component type has a Field[*Visibility].
// This is used at registration time to validate that archetypes using Visibility
// have configured a businessID alias.
func hasVisibilityField(componentT reflect.Type) bool {
	if componentT.Kind() == reflect.Pointer {
		componentT = componentT.Elem()
	}
	if componentT.Kind() != reflect.Struct {
		return false
	}
	for i := range componentT.NumField() {
		fieldT := componentT.Field(i).Type
		if fieldT == visibilityFieldT {
			return true
		}
	}
	return false
}
