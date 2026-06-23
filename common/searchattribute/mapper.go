//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination mapper_mock.go

package searchattribute

import (
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

type (
	// Mapper interface allows overriding custom search attribute names with aliases per namespace.
	// Create an instance of a Mapper interface and pass it to the temporal.NewServer using temporal.WithSearchAttributesMapper.
	// Returned error must be from the serviceerror package.
	Mapper interface {
		GetAlias(fieldName string, namespace string) (string, error)
		GetFieldName(alias string, namespace string) (string, error)
	}

	// NoopMapper is an identity mapper that returns all field names and aliases unchanged.
	NoopMapper struct{}

	// This mapper preserves legacy custom search attribute behavior by falling back
	// to identity mapping when the wrapped mapper misses but cluster metadata still
	// recognizes the name as a legacy custom search attribute.
	backCompMapper struct {
		mapper              Mapper
		fallbackNameTypeMap NameTypeMap
	}

	MapperProvider interface {
		GetMapper(nsName namespace.Name) (Mapper, error)
	}

	mapperProviderImpl struct {
		customMapper             Mapper
		namespaceRegistry        namespace.Registry
		searchAttributesProvider Provider
		fallbackIndexName        string
	}
)

var _ Mapper = (*NoopMapper)(nil)
var _ Mapper = (*backCompMapper)(nil)
var _ Mapper = (*namespace.CustomSearchAttributesMapper)(nil)
var _ MapperProvider = (*mapperProviderImpl)(nil)

func (*NoopMapper) GetAlias(fieldName string, _ string) (string, error) {
	return fieldName, nil
}

func (*NoopMapper) GetFieldName(alias string, _ string) (string, error) {
	return alias, nil
}

func (m *backCompMapper) GetAlias(fieldName string, namespaceName string) (string, error) {
	alias, firstErr := m.mapper.GetAlias(fieldName, namespaceName)
	if firstErr != nil {
		if !m.isLegacyCustomSearchAttribute(fieldName) {
			return "", firstErr
		}
		// this is a custom search attribute registered through cluster metadata.
		return fieldName, nil
	}
	return alias, nil
}

func (m *backCompMapper) GetFieldName(alias string, namespaceName string) (string, error) {
	fieldName, firstErr := m.mapper.GetFieldName(alias, namespaceName)
	if firstErr != nil {
		if !m.isLegacyCustomSearchAttribute(alias) {
			return "", firstErr
		}
		// this is a custom search attribute registered through cluster metadata.
		return alias, nil
	}
	return fieldName, nil
}

func (m *backCompMapper) isLegacyCustomSearchAttribute(name string) bool {
	_, err := m.fallbackNameTypeMap.getType(name, customCategory)
	return err == nil
}

func NewMapperProvider(
	customMapper Mapper,
	namespaceRegistry namespace.Registry,
	searchAttributesProvider Provider,
	fallbackIndexName string,
) MapperProvider {
	return &mapperProviderImpl{
		customMapper:             customMapper,
		namespaceRegistry:        namespaceRegistry,
		searchAttributesProvider: searchAttributesProvider,
		fallbackIndexName:        fallbackIndexName,
	}
}

func (m *mapperProviderImpl) GetMapper(nsName namespace.Name) (Mapper, error) {
	if m.customMapper != nil {
		return m.customMapper, nil
	}
	saMapper, err := m.namespaceRegistry.GetCustomSearchAttributesMapper(nsName)
	if err != nil {
		return nil, err
	}
	fallbackNameTypeMap := NameTypeMap{}
	if m.fallbackIndexName != "" {
		nameTypeMap, err := m.searchAttributesProvider.GetSearchAttributes(m.fallbackIndexName, false)
		if err != nil {
			return nil, fmt.Errorf("failed to load search attributes for fallback index %q: %w", m.fallbackIndexName, err)
		}
		fallbackNameTypeMap = legacyCustomSearchAttributes(nameTypeMap)
	}
	return &backCompMapper{
		mapper:              &saMapper,
		fallbackNameTypeMap: fallbackNameTypeMap,
	}, nil
}

func legacyCustomSearchAttributes(nameTypeMap NameTypeMap) NameTypeMap {
	legacyCustomSearchAttributes := make(map[string]enumspb.IndexedValueType)
	for name, valueType := range nameTypeMap.Custom() {
		if sadefs.IsPreallocatedCSAFieldName(name, valueType) {
			continue
		}
		legacyCustomSearchAttributes[name] = valueType
	}
	return NewNameTypeMap(legacyCustomSearchAttributes)
}

// AliasFields returns SearchAttributes struct where each custom search attribute name is replaced with alias.
// If no replacement where made, it returns nil which means that original SearchAttributes struct should be used.
func AliasFields(
	mapperProvider MapperProvider,
	searchAttributes *commonpb.SearchAttributes,
	namespaceName string,
) (*commonpb.SearchAttributes, error) {
	mapper, err := mapperProvider.GetMapper(namespace.Name(namespaceName))
	if err != nil {
		return nil, err
	}

	if len(searchAttributes.GetIndexedFields()) == 0 || mapper == nil {
		return searchAttributes, nil
	}

	newIndexedFields := make(map[string]*commonpb.Payload, len(searchAttributes.GetIndexedFields()))
	mapped := false
	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		if !sadefs.IsMappable(saName) {
			newIndexedFields[saName] = saPayload
			continue
		}

		aliasName, err := mapper.GetAlias(saName, namespaceName)
		if err != nil {
			// Silently ignore serviceerror.InvalidArgument because it indicates unmapped field (alias was deleted, for example).
			// IMPORTANT: AliasFields should never return serviceerror.InvalidArgument because it is used by Poll API and the error
			// goes through up to SDK, which shutdowns worker when it receives serviceerror.InvalidArgument as poll response.
			var invalidArgumentErr *serviceerror.InvalidArgument
			if errors.As(err, &invalidArgumentErr) {
				continue
			}
			return nil, err
		}
		if aliasName != saName {
			mapped = true
		}
		newIndexedFields[aliasName] = saPayload
	}

	// If no field name was mapped, return nil to save on clone operation on caller side.
	if !mapped {
		return searchAttributes, nil
	}
	return &commonpb.SearchAttributes{IndexedFields: newIndexedFields}, nil
}

// UnaliasFields returns SearchAttributes struct where each search attribute alias is replaced with field name.
func UnaliasFields(
	mapperProvider MapperProvider,
	searchAttributes *commonpb.SearchAttributes,
	namespaceName string,
) (*commonpb.SearchAttributes, error) {
	mapper, err := mapperProvider.GetMapper(namespace.Name(namespaceName))
	if err != nil {
		return nil, err
	}

	if len(searchAttributes.GetIndexedFields()) == 0 || mapper == nil {
		return searchAttributes, nil
	}

	newIndexedFields := make(map[string]*commonpb.Payload, len(searchAttributes.GetIndexedFields()))
	mapped := false
	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		if !sadefs.IsMappable(saName) {
			newIndexedFields[saName] = saPayload
			continue
		}

		fieldName, err := mapper.GetFieldName(saName, namespaceName)
		if err != nil {
			return nil, err
		}
		if fieldName != saName {
			mapped = true
		}
		newIndexedFields[fieldName] = saPayload
	}

	if !mapped {
		return searchAttributes, nil
	}

	return &commonpb.SearchAttributes{IndexedFields: newIndexedFields}, nil
}

// IsUserDefinedSearchAttribute returns true if alias refers to a user-defined custom search
// attribute rather than a synthetic one (e.g. the synthetic ScheduleId that maps to WorkflowId).
//
// Two independent checks are required because custom SAs can be registered in two ways:
//  1. Via UpdateNamespace with an explicit alias: stored in the Mapper as alias → field-name.
//     GetFieldName returns the underlying field name (different from the alias), so the SA is
//     identifiable even when it is absent from the NameTypeMap under the alias.
//  2. Via AddSearchAttributes without an alias: stored directly in NameTypeMap's custom map
//     under the alias itself. GetFieldName returns an error (no mapping exists), so the type
//     map is the only way to detect these.
func IsUserDefinedSearchAttribute(alias string, saMapper Mapper, saNameType NameTypeMap, ns string) bool {
	// Check 1: explicit alias mapping resolves to a different underlying field name.
	if mapped, err := saMapper.GetFieldName(alias, ns); err == nil && mapped != alias {
		return true
	}
	// Check 2: alias is registered as a custom SA in the type map (no alias mapping).
	_, ok := saNameType.Custom()[alias]
	return ok
}
