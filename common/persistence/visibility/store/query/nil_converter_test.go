package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/searchattribute"
)

func TestNilStoreQueryConverter_GetDatetimeFormat(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	require.Empty(t, queryConverter.GetDatetimeFormat())
}

func TestNilStoreQueryConverter_BuildParenExpr(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	out, err := queryConverter.BuildParenExpr(struct{}{})
	require.Nil(t, out)
	require.NoError(t, err)
}

func TestNilStoreQueryConverter_BuildNotExpr(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	out, err := queryConverter.BuildNotExpr(struct{}{})
	require.Nil(t, out)
	require.NoError(t, err)
}

func TestNilStoreQueryConverter_BuildAndExpr(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	out, err := queryConverter.BuildAndExpr(struct{}{})
	require.Nil(t, out)
	require.NoError(t, err)
}

func TestNilStoreQueryConverter_BuildOrExpr(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	out, err := queryConverter.BuildOrExpr(struct{}{})
	require.Nil(t, out)
	require.NoError(t, err)
}

func TestNilStoreQueryConverter_ConvertComparisonExpr(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	out, err := queryConverter.ConvertComparisonExpr("", nil, 123)
	require.Nil(t, out)
	require.NoError(t, err)
}

func TestNilStoreQueryConverter_ConvertKeywordComparisonExpr(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	out, err := queryConverter.ConvertKeywordComparisonExpr("", nil, 123)
	require.Nil(t, out)
	require.NoError(t, err)
}

func TestNilStoreQueryConverter_ConvertKeywordListComparisonExpr(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	out, err := queryConverter.ConvertKeywordListComparisonExpr("", nil, 123)
	require.Nil(t, out)
	require.NoError(t, err)
}

func TestNilStoreQueryConverter_ConvertTextComparisonExpr(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	out, err := queryConverter.ConvertTextComparisonExpr("", nil, 123)
	require.Nil(t, out)
	require.NoError(t, err)
}

func TestNilStoreQueryConverter_ConvertRangeExpr(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	out, err := queryConverter.ConvertRangeExpr("", nil, 123, 456)
	require.Nil(t, out)
	require.NoError(t, err)
}

func TestNilStoreQueryConverter_ConvertIsExpr(t *testing.T) {
	t.Parallel()
	queryConverter := &nilStoreQueryConverter{}
	out, err := queryConverter.ConvertIsExpr("", nil)
	require.Nil(t, out)
	require.NoError(t, err)
}

func TestNewNilQueryConverter(t *testing.T) {
	t.Parallel()
	c := NewNilQueryConverter("", searchattribute.TestNameTypeMap(), nil)
	require.Equal(t, &nilStoreQueryConverter{}, c.storeQC)
}
