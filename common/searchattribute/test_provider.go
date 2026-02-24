package searchattribute

import (
	"fmt"
	"maps"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type (
	TestProvider struct {
		es bool
	}

	TestMapper struct {
		Namespace            string
		WithCustomScheduleID bool
	}
)

var _ Provider = (*TestProvider)(nil)
var _ Mapper = (*TestMapper)(nil)

var (
	esCustomSearchAttributes = map[string]enumspb.IndexedValueType{
		"CustomIntField":      enumspb.INDEXED_VALUE_TYPE_INT,
		"CustomTextField":     enumspb.INDEXED_VALUE_TYPE_TEXT,
		"CustomKeywordField":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"CustomDatetimeField": enumspb.INDEXED_VALUE_TYPE_DATETIME,
		"CustomDoubleField":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
		"CustomBoolField":     enumspb.INDEXED_VALUE_TYPE_BOOL,
	}

	// default custom search attributes definition for SQL databases
	sqlCustomSearchAttributes = sadefs.GetDBIndexSearchAttributes(nil).CustomSearchAttributes

	// ScheduleId is mapped to Keyword10 for tests
	TestScheduleIDFieldName = "Keyword10"

	TestAliases = map[string]string{
		"Int01":         "CustomIntField",
		"Text01":        "CustomTextField",
		"Keyword01":     "CustomKeywordField",
		"Datetime01":    "CustomDatetimeField",
		"Double01":      "CustomDoubleField",
		"Bool01":        "CustomBoolField",
		"KeywordList01": "CustomKeywordListField",
	}
)

func TestNameTypeMap() NameTypeMap {
	csa := maps.Clone(sqlCustomSearchAttributes)
	return NameTypeMap{
		customSearchAttributes: csa,
	}
}

func TestEsNameTypeMap() NameTypeMap {
	csa := maps.Clone(esCustomSearchAttributes)
	return NameTypeMap{
		customSearchAttributes: csa,
	}
}

func TestEsNameTypeMapWithScheduleID() NameTypeMap {
	res := TestEsNameTypeMap()
	res.customSearchAttributes[sadefs.ScheduleID] = enumspb.INDEXED_VALUE_TYPE_KEYWORD
	return res
}

func NewTestProvider() *TestProvider {
	return &TestProvider{}
}

func NewTestEsProvider() *TestProvider {
	return &TestProvider{
		es: true,
	}
}

func (s *TestProvider) GetSearchAttributes(_ string, _ bool) (NameTypeMap, error) {
	if s.es {
		return TestEsNameTypeMap(), nil
	}
	return TestNameTypeMap(), nil
}

func (t *TestMapper) GetAlias(fieldName string, namespace string) (string, error) {
	if fieldName == "wrong_field" {
		// This error must be always ignored.
		return "", serviceerror.NewInvalidArgument("unmapped field")
	}
	if namespace == "error-namespace" {
		return "", serviceerror.NewInvalidArgument(
			fmt.Sprintf("Namespace %s has no mapping defined for field name %s", namespace, fieldName),
		)
	}
	if namespace == "test-namespace" || namespace == t.Namespace {
		if fieldName == "pass-through" {
			return fieldName, nil
		}
		if t.WithCustomScheduleID && fieldName == TestScheduleIDFieldName {
			return sadefs.ScheduleID, nil
		}
		return "AliasFor" + fieldName, nil
	}

	// This error must be always ignored.
	return "", serviceerror.NewInvalidArgument("unknown namespace")
}

func (t *TestMapper) GetFieldName(alias string, namespace string) (string, error) {
	if alias == "wrong_alias" {
		// This error must be always ignored.
		return "", serviceerror.NewInvalidArgument("unmapped alias")
	}
	if namespace == "error-namespace" {
		return "", serviceerror.NewInvalidArgument(
			fmt.Sprintf("Namespace %s has no mapping defined for search attribute %s", namespace, alias),
		)
	} else if namespace == "test-namespace" || namespace == t.Namespace {
		if alias == "pass-through" {
			return alias, nil
		}
		if t.WithCustomScheduleID && alias == sadefs.ScheduleID {
			return TestScheduleIDFieldName, nil
		}
		if strings.HasPrefix(alias, "AliasFor") {
			return strings.TrimPrefix(alias, "AliasFor"), nil
		} else if strings.HasPrefix(alias, "AliasWithHyphenFor-") {
			return strings.TrimPrefix(alias, "AliasWithHyphenFor-"), nil
		}
		return "", serviceerror.NewInvalidArgument("mapper error")
	}
	return "", serviceerror.NewInvalidArgument("unknown namespace")
}

func NewNoopMapper() Mapper {
	return &noopMapper{}
}

func NewTestMapperProvider(customMapper Mapper) MapperProvider {
	return NewMapperProvider(customMapper, nil, NewTestProvider(), false)
}

func NewNameTypeMapStub(attributes map[string]enumspb.IndexedValueType) NameTypeMap {
	return NameTypeMap{customSearchAttributes: attributes}
}
