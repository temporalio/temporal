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
			name:     "Map type",
			input:    Map[string, int]{},
			expected: chasmMapTypePrefix,
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

func (s *fieldsIteratorSuite) TestChasmMapTypePrefix() {
	c := Map[string, any]{}
	cT := reflect.TypeOf(c)
	s.True(strings.HasPrefix(cT.String(), chasmMapTypePrefix))
}

func (s *fieldsIteratorSuite) TestFieldsOf() {
	type fieldPointer struct {
		DataField    *protoMessageType
		InvalidField *Field[string]
	}

	type noDataField struct {
		SubField Field[string]
		SubMap   Map[string, int]
	}

	type twoDataFields struct {
		DataField        *protoMessageType
		AnotherDataField *protoMessageType
	}

	type unimplementedComponentOnly struct {
		UnimplementedComponent
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
				DataField *protoMessageType
				SubField  Field[string]
				SubMap    Map[string, int]
				ignored   *struct{}
			}{},
			expectedKinds:  []fieldKind{fieldKindData, fieldKindSubField, fieldKindSubMap},
			expectedNames:  []string{"DataField", "SubField", "SubMap"},
			expectedTypes:  []string{"*persistence.WorkflowExecutionState", "chasm.Field[string]", "chasm.Map[string,int]"},
			expectedErrors: []string{"", "", ""},
		},
		{
			name:           "Component with no data field",
			input:          &noDataField{},
			expectedKinds:  []fieldKind{fieldKindSubField, fieldKindSubMap, fieldKindUnspecified},
			expectedNames:  []string{"SubField", "SubMap", ""},
			expectedTypes:  []string{"chasm.Field[string]", "chasm.Map[string,int]", ""},
			expectedErrors: []string{"", "", "*chasm.noDataField: no data field (implements proto.Message) found"},
		},
		{
			name:           "Component with *Field",
			input:          &fieldPointer{},
			expectedKinds:  []fieldKind{fieldKindData, fieldKindUnspecified},
			expectedNames:  []string{"DataField", "InvalidField"},
			expectedTypes:  []string{"*persistence.WorkflowExecutionState", "*chasm.Field[string]"},
			expectedErrors: []string{"", "*chasm.fieldPointer.InvalidField: CHASM fields must not be pointers"},
		},
		{
			name:           "Component with multiple data fields",
			input:          &twoDataFields{},
			expectedKinds:  []fieldKind{fieldKindData, fieldKindData},
			expectedNames:  []string{"DataField", "AnotherDataField"},
			expectedTypes:  []string{"*persistence.WorkflowExecutionState", "*persistence.WorkflowExecutionState"},
			expectedErrors: []string{"", "*chasm.twoDataFields.AnotherDataField: only one data field DataField (implements proto.Message) allowed in component"},
		},
		{
			name:           "Component with UnimplementedComponent only",
			input:          &unimplementedComponentOnly{},
			expectedKinds:  []fieldKind{fieldKindUnspecified},
			expectedNames:  []string{""},
			expectedTypes:  []string{""},
			expectedErrors: []string{"*chasm.unimplementedComponentOnly: no data field (implements proto.Message) found"},
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

func (s *fieldsIteratorSuite) TestUnmanagedFieldsOf() {
	type unmanagedFields struct {
		UnimplementedComponent

		DataField  *protoMessageType
		SomeField  Field[string]
		unmanaged  struct{}
		anotherPtr *struct{}
	}

	var result []string
	for r := range unmanagedFieldsOf(reflect.TypeFor[unmanagedFields]()) {
		result = append(result, r.name)
	}
	s.Equal(2, len(result))
	s.ElementsMatch([]string{"unmanaged", "anotherPtr"}, result)
}
