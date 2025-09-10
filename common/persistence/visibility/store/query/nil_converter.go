package query

import (
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
)

type nilStoreQueryConverter struct{}

var _ StoreQueryConverter[any] = (*nilStoreQueryConverter)(nil)

func (c *nilStoreQueryConverter) GetDatetimeFormat() string {
	return ""
}

func (c *nilStoreQueryConverter) BuildParenExpr(expr any) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) BuildNotExpr(expr any) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) BuildAndExpr(exprs ...any) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) BuildOrExpr(exprs ...any) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertComparisonExpr(
	operator string,
	col *SAColName,
	value any,
) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertKeywordComparisonExpr(
	operator string,
	col *SAColName,
	value any,
) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *SAColName,
	value any,
) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertTextComparisonExpr(
	operator string,
	col *SAColName,
	value any,
) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertRangeExpr(
	operator string,
	col *SAColName,
	from, to any,
) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertIsExpr(operator string, col *SAColName) (any, error) {
	return nil, nil
}

type NilQueryConverter = *QueryConverter[any]

func NewNilQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
) NilQueryConverter {
	return NewQueryConverter(
		&nilStoreQueryConverter{},
		namespaceName,
		namespaceID,
		saTypeMap,
		saMapper,
	)
}
