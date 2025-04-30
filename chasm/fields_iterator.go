// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package chasm

import (
	"fmt"
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

func fieldsOf(valueV reflect.Value) iter.Seq[fieldInfo] {
	valueT := valueV.Type()
	dataFieldFound := false
	return func(yield func(fi fieldInfo) bool) {
		for i := 0; i < valueT.Elem().NumField(); i++ {
			fieldV := valueV.Elem().Field(i)
			fieldT := fieldV.Type()
			if fieldT == UnimplementedComponentT {
				continue
			}
			fieldN := fieldName(valueT.Elem().Field(i))

			var (
				fieldK   = fieldKindUnspecified
				fieldErr error
			)
			if fieldT.AssignableTo(protoMessageT) {
				if dataFieldFound {
					fieldErr = serviceerror.NewInternal("only one data field (implements proto.Message) allowed in component")
				}
				dataFieldFound = true
				fieldK = fieldKindData
			} else {
				prefix := genericTypePrefix(fieldT)
				switch prefix {
				case chasmFieldTypePrefix:
					fieldK = fieldKindSubField
				case chasmCollectionTypePrefix:
					fieldK = fieldKindSubCollection
				default:
					prefix = strings.TrimPrefix(prefix, "*")
					switch prefix {
					case chasmFieldTypePrefix:
						fieldErr = serviceerror.NewInternal(fmt.Sprintf("chasm field must be of type chasm.Field[T] not *chasm.Field[T] in component %s", valueT.String()))
					case chasmCollectionTypePrefix:
						fieldErr = serviceerror.NewInternal(fmt.Sprintf("chasm collection must be of type chasm.Collection[T] not *chasm.Collection[T] in component %s", valueT.String()))
					default:
						fieldErr = serviceerror.NewInternal(fmt.Sprintf("unsupported field type %s in component %s: must implement proto.Message, or be chasm.Field[T] or chasm.Collection[T]", fieldT.String(), valueT.String()))
					}
				}
			}

			if !yield(fieldInfo{val: fieldV, typ: fieldT, name: fieldN, kind: fieldK, err: fieldErr}) {
				return
			}
		}
		// If the data field is not found, generate one more fake field with only an error set.
		if !dataFieldFound {
			yield(fieldInfo{err: serviceerror.NewInternal(fmt.Sprintf("no data field (implements proto.Message) found in component %s", valueT.String()))})
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
