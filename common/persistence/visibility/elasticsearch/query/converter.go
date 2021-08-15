package query

import (
	"fmt"
	"strings"

	"github.com/olivere/elastic/v7"
	"github.com/xwb1989/sqlparser"
)

func ConvertWhereOrderBy(whereOrderBy string) (elastic.Query, []elastic.Sorter, error) {
	whereOrderBy = strings.Trim(whereOrderBy, " ")
	if whereOrderBy != "" && !strings.HasPrefix(whereOrderBy, "order by ") {
		whereOrderBy = "where " + whereOrderBy
	}
	sql := fmt.Sprintf("select * from table1 %s", whereOrderBy)
	return convert(sql)
}

// convert transforms sql to Elasticsearch query.
func convert(sql string) (elastic.Query, []elastic.Sorter, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", MalformedSqlQueryErr, err)
	}

	selectStmt, isSelect := stmt.(*sqlparser.Select)
	if !isSelect {
		return nil, nil, fmt.Errorf("%w: statement must be 'select' not %T", NotSupportedErr, stmt)
	}

	return handleSelect(selectStmt)
}
