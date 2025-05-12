// The following helpers are slightly modified versions of those found in testify's assert package
package protoassert

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/api/temporalproto"
)

func formatListDiff(listA, listB any, extraA, extraB any) string {
	var msg bytes.Buffer

	msg.WriteString("elements differ")
	if !isEmpty(extraA) {
		msg.WriteString("\n\nextra elements in list A:\n")
		msg.WriteString(prettyPrint(extraA))
	}
	if !isEmpty(extraB) {
		msg.WriteString("\n\nextra elements in list B:\n")
		msg.WriteString(prettyPrint(extraB))
	}
	msg.WriteString("\n\nlistA:\n")
	msg.WriteString(prettyPrint(listA))
	msg.WriteString("\n\nlistB:\n")
	msg.WriteString(prettyPrint(listB))

	return msg.String()
}

func diffLists(listA, listB interface{}) (extraA, extraB []interface{}) {
	aValue := reflect.ValueOf(listA)
	bValue := reflect.ValueOf(listB)

	aLen := aValue.Len()
	bLen := bValue.Len()

	// Mark indexes in bValue that we already used
	visited := make([]bool, bLen)
	for i := 0; i < aLen; i++ {
		element := aValue.Index(i).Interface()
		found := false
		for j := 0; j < bLen; j++ {
			if visited[j] {
				continue
			}
			if temporalproto.DeepEqual(bValue.Index(j).Interface(), element) {
				visited[j] = true
				found = true
				break
			}
		}
		if !found {
			extraA = append(extraA, element)
		}
	}

	for j := 0; j < bLen; j++ {
		if visited[j] {
			continue
		}
		extraB = append(extraB, bValue.Index(j).Interface())
	}

	return
}

func isEmpty(object interface{}) bool {

	// get nil case out of the way
	if object == nil {
		return true
	}

	objValue := reflect.ValueOf(object)

	switch objValue.Kind() {
	// collection types are empty when they have no element
	case reflect.Chan, reflect.Map, reflect.Slice:
		return objValue.Len() == 0
	// pointers are empty if nil or if the value they point to is empty
	case reflect.Ptr:
		if objValue.IsNil() {
			return true
		}
		deref := objValue.Elem().Interface()
		return isEmpty(deref)
	// for all other types, compare against the zero value
	// array types are empty when they match their zero-initialized state
	default:
		zero := reflect.Zero(objValue.Type())
		return reflect.DeepEqual(object, zero.Interface())
	}
}

// isList checks that the provided value is array or slice.
func isList(t assert.TestingT, list interface{}, msgAndArgs ...interface{}) (ok bool) {
	kind := reflect.TypeOf(list).Kind()
	if kind != reflect.Array && kind != reflect.Slice {
		return assert.Fail(t, fmt.Sprintf("%q has an unsupported type %s, expecting array or slice", list, kind),
			msgAndArgs...)
	}
	return true
}
