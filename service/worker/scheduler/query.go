package scheduler

import (
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	expmaps "golang.org/x/exp/maps"
)

type (
	fieldNameAggInterceptor struct {
		baseInterceptor query.FieldNameInterceptor
		names           map[string]bool
	}
)

var _ query.FieldNameInterceptor = (*fieldNameAggInterceptor)(nil)

func (i *fieldNameAggInterceptor) Name(name string, usage query.FieldNameUsage) (string, error) {
	i.names[name] = true
	return i.baseInterceptor.Name(name, usage)
}

func newFieldNameAggInterceptor(
	namespaceName namespace.Name,
	saNameType searchattribute.NameTypeMap,
	saMapperProvider searchattribute.MapperProvider,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) *fieldNameAggInterceptor {
	return &fieldNameAggInterceptor{
		baseInterceptor: elasticsearch.NewNameInterceptor(namespaceName, saNameType, saMapperProvider, chasmMapper, archetypeID),
		names:           make(map[string]bool),
	}
}

type saAggInterceptor struct {
	names map[string]struct{}
}

var _ query.SearchAttributeInterceptor = (*saAggInterceptor)(nil)

func newSaAggInterceptor() *saAggInterceptor {
	return &saAggInterceptor{
		names: make(map[string]struct{}),
	}
}

func (i *saAggInterceptor) Intercept(col *query.SAColumn) error {
	i.names[col.Alias] = struct{}{}
	return nil
}

func ValidateVisibilityQuery(
	namespaceName namespace.Name,
	saNameType searchattribute.NameTypeMap,
	saMapperProvider searchattribute.MapperProvider,
	enableUnifiedQueryConverter dynamicconfig.BoolPropertyFn,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) error {
	var fields []string
	var err error
	if enableUnifiedQueryConverter() {
		fields, err = getQueryFields(
			namespaceName,
			saNameType,
			saMapperProvider,
			queryString,
			chasmMapper,
			archetypeID,
		)
	} else {
		fields, err = getQueryFieldsLegacy(namespaceName, saNameType, saMapperProvider, queryString, chasmMapper, archetypeID)
	}
	if err != nil {
		return err
	}
	for _, field := range fields {
		if sadefs.IsReserved(field) && field != sadefs.TemporalSchedulePaused {
			return serviceerror.NewInvalidArgument(
				fmt.Sprintf("invalid query filter for schedules: cannot filter on %q", field),
			)
		}
	}
	return nil
}

func getQueryFields(
	namespaceName namespace.Name,
	saNameType searchattribute.NameTypeMap,
	saMapperProvider searchattribute.MapperProvider,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) ([]string, error) {
	saMapper, err := saMapperProvider.GetMapper(namespaceName)
	if err != nil {
		return nil, err
	}
	saInterceptor := newSaAggInterceptor()
	queryConverter := query.NewNilQueryConverter(
		namespaceName,
		saNameType,
		saMapper,
	).WithSearchAttributeInterceptor(saInterceptor).
		WithChasmMapper(chasmMapper).
		WithArchetypeID(archetypeID)
	_, err = queryConverter.Convert(queryString)
	if err != nil {
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}
	if !queryConverter.SeenNamespaceDivision() {
		delete(saInterceptor.names, sadefs.TemporalNamespaceDivision)
	}
	return expmaps.Keys(saInterceptor.names), nil
}

func getQueryFieldsLegacy(
	namespaceName namespace.Name,
	saNameType searchattribute.NameTypeMap,
	saMapperProvider searchattribute.MapperProvider,
	queryString string,
	chasmMapper *chasm.VisibilitySearchAttributesMapper,
	archetypeID chasm.ArchetypeID,
) ([]string, error) {
	fnInterceptor := newFieldNameAggInterceptor(namespaceName, saNameType, saMapperProvider, chasmMapper, archetypeID)
	queryConverter := elasticsearch.NewQueryConverterLegacy(fnInterceptor, nil, saNameType, chasmMapper)
	_, err := queryConverter.ConvertWhereOrderBy(queryString)
	if err != nil {
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}
	return expmaps.Keys(fnInterceptor.names), nil
}
