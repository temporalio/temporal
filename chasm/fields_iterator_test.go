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
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type fieldsIteratorSuite struct {
	suite.Suite
	*require.Assertions

	controller *gomock.Controller
}

func TestFieldsIteratorSuite(t *testing.T) {
	suite.Run(t, new(fieldsIteratorSuite))
}

func (s *fieldsIteratorSuite) SetupTest() {
	s.initAssertions()
	s.controller = gomock.NewController(s.T())
}

func (s *fieldsIteratorSuite) SetupSubTest() {
	s.initAssertions()
}

func (s *fieldsIteratorSuite) initAssertions() {
	// `s.Assertions` (as well as other test helpers which depends on `s.T()`) must be initialized on
	// both test and subtest levels (but not suite level, where `s.T()` is `nil`).
	//
	// If these helpers are not reinitialized on subtest level, any failed `assert` in
	// subtest will fail the entire test (not subtest) immediately without running other subtests.

	s.Assertions = require.New(s.T())
}

func (s *fieldsIteratorSuite) TestGenericTypePrefix() {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{
			name:     "Field type",
			input:    Field[string]{},
			expected: chasmFieldTypePrefix,
		},
		{
			name:     "Collection type",
			input:    Collection[int]{},
			expected: chasmCollectionTypePrefix,
		},
		{
			name:     "Non-generic type",
			input:    0,
			expected: "",
		},
		{
			name:     "Map type",
			input:    map[string]int{},
			expected: "map[",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			typ := reflect.TypeOf(tt.input)
			result := genericTypePrefix(typ)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *fieldsIteratorSuite) TestChasmFieldTypePrefix() {
	f := Field[any]{}
	fT := reflect.TypeOf(f)
	s.True(strings.HasPrefix(fT.String(), chasmFieldTypePrefix))
}

func (s *fieldsIteratorSuite) TestChasmCollectionTypePrefix() {
	c := Collection[any]{}
	cT := reflect.TypeOf(c)
	s.True(strings.HasPrefix(cT.String(), chasmCollectionTypePrefix))
}

func (s *fieldsIteratorSuite) TestFieldsOf() {
	type fieldPointer struct {
		DataField    *protoMessageType
		InvalidField *Field[string]
	}

	type noData struct {
		SubField      Field[string]
		SubCollection Collection[int]
	}

	tests := []struct {
		name           string
		input          any
		expectedKinds  []fieldKind
		expectedNames  []string
		expectedTypes  []string
		expectedErrors []string
	}{
		{
			name: "Valid component with one data field",
			input: &struct {
				UnimplementedComponent
				DataField     *protoMessageType
				SubField      Field[string]
				SubCollection Collection[int]
			}{},
			expectedKinds:  []fieldKind{fieldKindData, fieldKindSubField, fieldKindSubCollection},
			expectedNames:  []string{"DataField", "SubField", "SubCollection"},
			expectedTypes:  []string{"*persistence.ActivityInfo", "chasm.Field[string]", "chasm.Collection[int]"},
			expectedErrors: []string{"", "", ""},
		},
		{
			name:           "Component with no data field",
			input:          &noData{},
			expectedKinds:  []fieldKind{fieldKindSubField, fieldKindSubCollection, fieldKindUnspecified},
			expectedNames:  []string{"SubField", "SubCollection", ""},
			expectedTypes:  []string{"chasm.Field[string]", "chasm.Collection[int]", ""},
			expectedErrors: []string{"", "", "no data field (implements proto.Message) found in component *chasm.noData"},
		},
		{
			name:           "Component with *Field",
			input:          &fieldPointer{},
			expectedKinds:  []fieldKind{fieldKindData, fieldKindUnspecified},
			expectedNames:  []string{"DataField", "InvalidField"},
			expectedTypes:  []string{"*persistence.ActivityInfo", "*chasm.Field[string]"},
			expectedErrors: []string{"", "chasm field must be of type chasm.Field[T] not *chasm.Field[T] in component *chasm.fieldPointer"},
		},
		{
			name: "Component with multiple data fields",
			input: &struct {
				DataField        *protoMessageType
				AnotherDataField *protoMessageType
			}{},
			expectedKinds:  []fieldKind{fieldKindData, fieldKindData},
			expectedNames:  []string{"DataField", "AnotherDataField"},
			expectedTypes:  []string{"*persistence.ActivityInfo", "*persistence.ActivityInfo"},
			expectedErrors: []string{"", "only one data field (implements proto.Message) allowed in component"},
		},
		{
			name: "Component with UnimplementedComponent only",
			input: &struct {
				UnimplementedComponent
			}{},
			expectedKinds:  []fieldKind{fieldKindUnspecified},
			expectedNames:  []string{""},
			expectedTypes:  []string{""},
			expectedErrors: []string{"no data field (implements proto.Message) found in component *struct { chasm.UnimplementedComponent }"},
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			valueV := reflect.ValueOf(tt.input)

			var actualKinds []fieldKind
			var actualNames []string
			var actualTypes []string
			var actualErrors []string

			for field := range fieldsOf(valueV) {
				actualKinds = append(actualKinds, field.kind)
				actualNames = append(actualNames, field.name)
				if field.typ != nil {
					actualTypes = append(actualTypes, field.typ.String())
				} else {
					actualTypes = append(actualTypes, "")
				}
				if field.err != nil {
					actualErrors = append(actualErrors, field.err.Error())
				} else {
					actualErrors = append(actualErrors, "")
				}
			}

			s.Equal(tt.expectedKinds, actualKinds)
			s.Equal(tt.expectedNames, actualNames)
			s.Equal(tt.expectedTypes, actualTypes)
			s.Equal(tt.expectedErrors, actualErrors)
		})
	}
}
