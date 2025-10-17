package searchattribute

import (
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
)

type (
	TestProvider struct{}

	TestMapper struct {
		Namespace string
	}
)

var _ Provider = (*TestProvider)(nil)
var _ Mapper = (*TestMapper)(nil)

var (
	TestNameTypeMap = NameTypeMap{
		customSearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomIntField":      enumspb.INDEXED_VALUE_TYPE_INT,
			"CustomTextField":     enumspb.INDEXED_VALUE_TYPE_TEXT,
			"CustomKeywordField":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"CustomDatetimeField": enumspb.INDEXED_VALUE_TYPE_DATETIME,
			"CustomDoubleField":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			"CustomBoolField":     enumspb.INDEXED_VALUE_TYPE_BOOL,

			"Int01":         enumspb.INDEXED_VALUE_TYPE_INT,
			"Int02":         enumspb.INDEXED_VALUE_TYPE_INT,
			"Int03":         enumspb.INDEXED_VALUE_TYPE_INT,
			"Text01":        enumspb.INDEXED_VALUE_TYPE_TEXT,
			"Keyword01":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"Keyword02":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"Keyword03":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"Datetime01":    enumspb.INDEXED_VALUE_TYPE_DATETIME,
			"Double01":      enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			"Bool01":        enumspb.INDEXED_VALUE_TYPE_BOOL,
			"KeywordList01": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		},
	}

	TestNameTypeMapWithScheduleId = NameTypeMap{
		customSearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomIntField":      enumspb.INDEXED_VALUE_TYPE_INT,
			"CustomTextField":     enumspb.INDEXED_VALUE_TYPE_TEXT,
			"CustomKeywordField":  enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"CustomDatetimeField": enumspb.INDEXED_VALUE_TYPE_DATETIME,
			"CustomDoubleField":   enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			"CustomBoolField":     enumspb.INDEXED_VALUE_TYPE_BOOL,

			"Int01":         enumspb.INDEXED_VALUE_TYPE_INT,
			"Int02":         enumspb.INDEXED_VALUE_TYPE_INT,
			"Int03":         enumspb.INDEXED_VALUE_TYPE_INT,
			"Text01":        enumspb.INDEXED_VALUE_TYPE_TEXT,
			"Keyword01":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"Keyword02":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"Keyword03":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
			"Datetime01":    enumspb.INDEXED_VALUE_TYPE_DATETIME,
			"Double01":      enumspb.INDEXED_VALUE_TYPE_DOUBLE,
			"Bool01":        enumspb.INDEXED_VALUE_TYPE_BOOL,
			"KeywordList01": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
			ScheduleID:      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	}

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

func NewTestProvider() *TestProvider {
	return &TestProvider{}
}

func (s *TestProvider) GetSearchAttributes(_ string, _ bool) (NameTypeMap, error) {
	return TestNameTypeMap, nil
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
		if strings.HasPrefix(alias, "AliasFor") {
			return strings.TrimPrefix(alias, "AliasFor"), nil
		} else if strings.HasPrefix(alias, "AliasWithHyphenFor-") {
			return strings.TrimPrefix(alias, "AliasWithHyphenFor-"), nil
		}
		return "", serviceerror.NewInvalidArgument("mapper error")
	}
	return "", serviceerror.NewInvalidArgument("unknown namespace")
}

func NewTestMapperProvider(customMapper Mapper) MapperProvider {
	return NewMapperProvider(customMapper, nil, NewTestProvider(), false)
}

func NewNameTypeMapStub(attributes map[string]enumspb.IndexedValueType) NameTypeMap {
	return NameTypeMap{customSearchAttributes: attributes}
}
