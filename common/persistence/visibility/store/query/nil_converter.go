package query

import (
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
)

// nilStoreQueryConverter is a dummy query converter implementation that returns nil value as the
// result of converting the query.
// It can be useful if you just need to traverse the query and collect information using the
// SearchAttributeInterceptor.
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
	col *SAColumn,
	value any,
) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertKeywordComparisonExpr(
	operator string,
	col *SAColumn,
	value any,
) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertKeywordListComparisonExpr(
	operator string,
	col *SAColumn,
	value any,
) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertTextComparisonExpr(
	operator string,
	col *SAColumn,
	value any,
) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertRangeExpr(
	operator string,
	col *SAColumn,
	from, to any,
) (any, error) {
	return nil, nil
}

func (c *nilStoreQueryConverter) ConvertIsExpr(operator string, col *SAColumn) (any, error) {
	return nil, nil
}

type NilQueryConverter = *QueryConverter[any]

func NewNilQueryConverter(
	namespaceName namespace.Name,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
) NilQueryConverter {
	return NewQueryConverter(
		&nilStoreQueryConverter{},
		namespaceName,
		saTypeMap,
		saMapper,
	)
}
