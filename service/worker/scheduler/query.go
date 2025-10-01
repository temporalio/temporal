package scheduler

import (
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
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
) *fieldNameAggInterceptor {
	return &fieldNameAggInterceptor{
		baseInterceptor: elasticsearch.NewNameInterceptor(namespaceName, saNameType, saMapperProvider),
		names:           make(map[string]bool),
	}
}

func ValidateVisibilityQuery(
	namespaceName namespace.Name,
	saNameType searchattribute.NameTypeMap,
	saMapperProvider searchattribute.MapperProvider,
	queryString string,
) error {
	fields, err := getQueryFields(namespaceName, saNameType, saMapperProvider, queryString)
	if err != nil {
		return err
	}
	for _, field := range fields {
		if searchattribute.IsReserved(field) && field != searchattribute.TemporalSchedulePaused {
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
) ([]string, error) {
	fnInterceptor := newFieldNameAggInterceptor(namespaceName, saNameType, saMapperProvider)
	queryConverter := elasticsearch.NewQueryConverterLegacy(fnInterceptor, nil, saNameType)
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
