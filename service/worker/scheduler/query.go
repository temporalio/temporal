package scheduler

import (
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	expmaps "golang.org/x/exp/maps"
)

type saAggInterceptor struct {
	names map[string]bool
}

var _ query.SearchAttributeInterceptor = (*saAggInterceptor)(nil)

func newSaAggInterceptor() *saAggInterceptor {
	return &saAggInterceptor{
		names: make(map[string]bool),
	}
}

func (i *saAggInterceptor) Intercept(col *query.SAColName) error {
	i.names[col.Alias] = true
	return nil
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
	saMapper, err := saMapperProvider.GetMapper(namespaceName)
	if err != nil {
		return nil, err
	}
	saInterceptor := newSaAggInterceptor()
	queryConverter := query.NewNilQueryConverter(
		namespaceName,
		"", // namespace ID is not needed in this query converter
		saNameType,
		saMapper,
	).WithSearchAttributeInterceptor(saInterceptor)
	_, err = queryConverter.Convert(queryString)
	if err != nil {
		var converterErr *query.ConverterError
		if errors.As(err, &converterErr) {
			return nil, converterErr.ToInvalidArgument()
		}
		return nil, err
	}
	return expmaps.Keys(saInterceptor.names), nil
}
