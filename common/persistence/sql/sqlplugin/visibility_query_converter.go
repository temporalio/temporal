//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination visibility_query_converter_mock.go

package sqlplugin

import (
	"encoding/json"
	"time"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/persistence/visibility/store/query"
)

type (
	VisibilityPageToken struct {
		CloseTime time.Time
		StartTime time.Time
		RunID     string
	}
)

func DeserializeVisibilityPageToken(data []byte) (*VisibilityPageToken, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var token *VisibilityPageToken
	err := json.Unmarshal(data, &token)
	return token, err
}

func SerializeVisibilityPageToken(token *VisibilityPageToken) ([]byte, error) {
	data, err := json.Marshal(token)
	return data, err
}

type VisibilityQueryConverter interface {
	GetDatetimeFormat() string

	ConvertKeywordListComparisonExpr(
		operator string,
		col *query.SAColumn,
		value sqlparser.Expr,
	) (sqlparser.Expr, error)

	ConvertTextComparisonExpr(
		operator string,
		col *query.SAColumn,
		value sqlparser.Expr,
	) (sqlparser.Expr, error)

	BuildSelectStmt(
		queryExpr *query.QueryParams[sqlparser.Expr],
		pageSize int,
		pageToken *VisibilityPageToken,
	) (string, []any)

	BuildCountStmt(
		queryExpr *query.QueryParams[sqlparser.Expr],
	) (string, []any)
}
