package searchattribute

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

func Test_AliasFields(t *testing.T) {
	mapperProvider := NewTestMapperProvider(&TestMapper{})

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"Field1":      {Data: []byte("data1")},
			"wrong_field": {Data: []byte("data23")}, // Wrong unknown name must be ignored.
		},
	}
	// Fields without an alias are dropped, even when no other field is renamed.
	sb, err := AliasFields(mapperProvider, sa, "error-namespace")
	require.NoError(t, err)
	require.Empty(t, sb.GetIndexedFields())

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"Field1":      {Data: []byte("data1")},
			"wrong_field": {Data: []byte("data23")}, // Wrong unknown name must be ignored.
		},
	}
	sb, err = AliasFields(mapperProvider, sa, "unknown-namespace")
	require.NoError(t, err)
	require.Empty(t, sb.GetIndexedFields())

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"Field1":      {Data: []byte("data1")},
			"Field2":      {Data: []byte("data2")},
			"wrong_field": {Data: []byte("data23")}, // Wrong unknown name must be ignored.
		},
	}
	sb, err = AliasFields(mapperProvider, sa, "test-namespace")
	require.NoError(t, err)
	require.NotEqual(t, sa, sb)
	require.Len(t, sb.GetIndexedFields(), 2)
	require.EqualValues(t, "data1", sb.GetIndexedFields()["AliasForField1"].GetData())
	require.EqualValues(t, "data2", sb.GetIndexedFields()["AliasForField2"].GetData())

	// Empty search attributes are not validated with mapper.
	sa = &commonpb.SearchAttributes{
		IndexedFields: nil,
	}
	sb, err = AliasFields(mapperProvider, sa, "error-namespace")
	require.NoError(t, err)
	require.Equal(t, sa, sb)
	sb, err = AliasFields(mapperProvider, sa, "unknown-namespace")
	require.NoError(t, err)
	require.Equal(t, sa, sb)

	// Pass through search attributes are not mapped.
	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"pass-through": {Data: []byte("data1")},
		},
	}
	sb, err = AliasFields(mapperProvider, sa, "test-namespace")
	require.NoError(t, err)
	require.Equal(t, sa, sb)
}

func Test_UnaliasFields(t *testing.T) {
	mapperProvider := NewTestMapperProvider(&TestMapper{})

	sa := &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"AliasForField1": {Data: []byte("data1")},
		},
	}
	_, err := UnaliasFields(mapperProvider, sa, "error-namespace")
	require.Error(t, err)
	var invalidArgumentErr2 *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgumentErr2)

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"AliasForField1": {Data: []byte("data1")},
			"AliasForField2": {Data: []byte("data2")},
		},
	}
	_, err = UnaliasFields(mapperProvider, sa, "unknown-namespace")
	require.Error(t, err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgumentErr)

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"AliasForField1": {Data: []byte("data1")},
			"AliasForField2": {Data: []byte("data2")},
		},
	}
	sa, err = UnaliasFields(mapperProvider, sa, "test-namespace")
	require.NoError(t, err)
	require.NotNil(t, sa)
	require.Len(t, sa.GetIndexedFields(), 2)
	require.EqualValues(t, "data1", sa.GetIndexedFields()["Field1"].GetData())
	require.EqualValues(t, "data2", sa.GetIndexedFields()["Field2"].GetData())

	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"AliasForField1": {Data: []byte("data1")},
			"AliasForField2": {Data: []byte("data2")},
			"wrong_alias":    {Data: []byte("data3")},
		},
	}
	_, err = UnaliasFields(mapperProvider, sa, "test-namespace")
	require.Error(t, err)
	require.ErrorAs(t, err, &invalidArgumentErr)

	// Empty search attributes are not validated with mapper.
	sa = &commonpb.SearchAttributes{
		IndexedFields: nil,
	}
	sb, err := UnaliasFields(mapperProvider, sa, "error-namespace")
	require.NoError(t, err)
	require.Equal(t, sa, sb, "when there is nothin to unalias should return received attributes")

	sb, err = UnaliasFields(mapperProvider, sa, "unknown-namespace")
	require.NoError(t, err)
	require.Equal(t, sa, sb, "when there is nothin to unalias should return received attributes")

	// Pass through aliases are not substituted.
	sa = &commonpb.SearchAttributes{
		IndexedFields: map[string]*commonpb.Payload{
			"pass-through": {Data: []byte("data1")},
		},
	}
	sb, err = UnaliasFields(mapperProvider, sa, "test-namespace")
	require.NoError(t, err)
	require.Equal(t, sa, sb, "when there is nothin to unalias should return received attributes")
}

type staticSearchAttributesProvider struct {
	nameTypeMaps map[string]NameTypeMap
	err          error
}

func (s staticSearchAttributesProvider) GetSearchAttributes(indexName string, _ bool) (NameTypeMap, error) {
	if s.err != nil {
		return NameTypeMap{}, s.err
	}
	if nameTypeMap, ok := s.nameTypeMaps[indexName]; ok {
		return nameTypeMap, nil
	}
	return NameTypeMap{}, nil
}

func Test_BackCompMapperFallsBackToClusterMetadataFields(t *testing.T) {
	mapper := &backCompMapper{
		mapper:              &TestMapper{},
		fallbackNameTypeMap: TestNameTypeMap(),
	}

	alias, err := mapper.GetAlias("Keyword02", "error-namespace")
	require.NoError(t, err)
	require.Equal(t, "Keyword02", alias)

	fieldName, err := mapper.GetFieldName("Keyword02", "error-namespace")
	require.NoError(t, err)
	require.Equal(t, "Keyword02", fieldName)
}

func TestMapperProviderUsesConfiguredVisibilityIndexForBackCompatFallback(t *testing.T) {
	controller := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(controller)
	nsRegistry.EXPECT().
		GetCustomSearchAttributesMapper(namespace.Name("test-namespace")).
		Return(namespace.CustomSearchAttributesMapper{}, nil)

	mapperProvider := NewMapperProvider(
		nil,
		nsRegistry,
		staticSearchAttributesProvider{
			nameTypeMaps: map[string]NameTypeMap{
				"test-visibility-index": NewNameTypeMap(map[string]enumspb.IndexedValueType{
					"LegacyKeyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
				}),
			},
		},
		"test-visibility-index",
	)

	mapper, err := mapperProvider.GetMapper(namespace.Name("test-namespace"))
	require.NoError(t, err)

	alias, err := mapper.GetAlias("LegacyKeyword", "error-namespace")
	require.NoError(t, err)
	require.Equal(t, "LegacyKeyword", alias)

	fieldName, err := mapper.GetFieldName("LegacyKeyword", "error-namespace")
	require.NoError(t, err)
	require.Equal(t, "LegacyKeyword", fieldName)
}

func TestMapperProviderDoesNotTreatPreallocatedFieldsAsLegacyCustomAttributes(t *testing.T) {
	controller := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(controller)
	nsRegistry.EXPECT().
		GetCustomSearchAttributesMapper(namespace.Name("test-namespace")).
		Return(namespace.CustomSearchAttributesMapper{}, nil)

	mapperProvider := NewMapperProvider(
		nil,
		nsRegistry,
		staticSearchAttributesProvider{
			nameTypeMaps: map[string]NameTypeMap{
				"test-visibility-index": TestNameTypeMap(),
			},
		},
		"test-visibility-index",
	)

	mapper, err := mapperProvider.GetMapper(namespace.Name("test-namespace"))
	require.NoError(t, err)

	_, err = mapper.GetFieldName("Text01", "error-namespace")
	require.Error(t, err)
	var invalidArgumentErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgumentErr)
}

func TestMapperProviderReturnsFallbackLookupError(t *testing.T) {
	controller := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(controller)
	nsRegistry.EXPECT().
		GetCustomSearchAttributesMapper(namespace.Name("test-namespace")).
		Return(namespace.CustomSearchAttributesMapper{}, nil)

	expectedErr := errors.New("boom")
	mapperProvider := NewMapperProvider(
		nil,
		nsRegistry,
		staticSearchAttributesProvider{
			err: expectedErr,
		},
		"test-visibility-index",
	)

	_, err := mapperProvider.GetMapper(namespace.Name("test-namespace"))
	require.Error(t, err)
	require.ErrorContains(t, err, `failed to load search attributes for fallback index "test-visibility-index"`)
	require.ErrorContains(t, err, expectedErr.Error())
}

type staticAliasMapper struct {
	aliasToField map[string]string
}

func (m staticAliasMapper) GetAlias(fieldName string, namespaceName string) (string, error) {
	for alias, field := range m.aliasToField {
		if field == fieldName {
			return alias, nil
		}
	}
	return "", serviceerror.NewInvalidArgumentf("namespace %s has no mapping for field %s", namespaceName, fieldName)
}

func (m staticAliasMapper) GetFieldName(alias string, namespaceName string) (string, error) {
	if field, ok := m.aliasToField[alias]; ok {
		return field, nil
	}
	return "", serviceerror.NewInvalidArgumentf("namespace %s has no mapping for alias %s", namespaceName, alias)
}

func Test_BackCompMapperPrefersLegacyCustomSearchAttributeOverNamespaceAlias(t *testing.T) {
	mapper := &backCompMapper{
		mapper: staticAliasMapper{aliasToField: map[string]string{
			// Left over from a SQL visibility store: same name as the legacy attribute below.
			"CustomerId": "Keyword01",
			"NewAttr":    "Keyword02",
		}},
		fallbackNameTypeMap: NewNameTypeMap(map[string]enumspb.IndexedValueType{
			"CustomerId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		}),
	}

	fieldName, err := mapper.GetFieldName("CustomerId", "ns")
	require.NoError(t, err)
	require.Equal(t, "CustomerId", fieldName)

	fieldName, err = mapper.GetFieldName("NewAttr", "ns")
	require.NoError(t, err)
	require.Equal(t, "Keyword02", fieldName)

	_, err = mapper.GetFieldName("Unknown", "ns")
	var invalidArgumentErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgumentErr)

	alias, err := mapper.GetAlias("CustomerId", "ns")
	require.NoError(t, err)
	require.Equal(t, "CustomerId", alias)

	alias, err = mapper.GetAlias("Keyword02", "ns")
	require.NoError(t, err)
	require.Equal(t, "NewAttr", alias)

	// The field behind the shadowed alias has no alias, so it doesn't collide with the legacy
	// attribute.
	_, err = mapper.GetAlias("Keyword01", "ns")
	require.ErrorAs(t, err, &invalidArgumentErr)

	for _, tc := range []struct {
		name     string
		input    map[string]string
		expected map[string]string
	}{
		{
			name:     "with another renamed field",
			input:    map[string]string{"CustomerId": "legacy", "Keyword01": "via-alias", "Keyword02": "new"},
			expected: map[string]string{"CustomerId": "legacy", "NewAttr": "new"},
		},
		{
			// The result doesn't depend on whether another field is renamed.
			name:     "without another renamed field",
			input:    map[string]string{"CustomerId": "legacy", "Keyword01": "via-alias"},
			expected: map[string]string{"CustomerId": "legacy"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			indexedFields := make(map[string]*commonpb.Payload, len(tc.input))
			for name, value := range tc.input {
				indexedFields[name] = &commonpb.Payload{Data: []byte(value)}
			}
			sa, err := AliasFields(
				staticMapperProvider{mapper: mapper},
				&commonpb.SearchAttributes{IndexedFields: indexedFields},
				"ns",
			)
			require.NoError(t, err)
			result := make(map[string]string, len(sa.GetIndexedFields()))
			for name, payload := range sa.GetIndexedFields() {
				result[name] = string(payload.GetData())
			}
			require.Equal(t, tc.expected, result)
		})
	}
}

type staticMapperProvider struct {
	mapper Mapper
}

func (p staticMapperProvider) GetMapper(namespace.Name) (Mapper, error) {
	return p.mapper, nil
}

func TestWithoutPreallocatedFields(t *testing.T) {
	result := WithoutPreallocatedFields(map[string]enumspb.IndexedValueType{
		"Keyword01":     enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"KeywordList03": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST,
		"Text02":        enumspb.INDEXED_VALUE_TYPE_TEXT,
		// Same pattern but a different type than the pre-allocated field: registered by a user.
		"Int01":      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"CustomerId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	})
	require.Equal(t, map[string]enumspb.IndexedValueType{
		"Int01":      enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		"CustomerId": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
	}, result)
}
