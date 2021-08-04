package query

import (
	"errors"

	"github.com/olivere/elastic/v7"
	"github.com/xwb1989/sqlparser"
)

// Convert will transform sql to elasticsearch dsl string
func Convert(sql string) (elastic.Query, []elastic.Sorter, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, nil, err
	}

	selectStmt, isSelect := stmt.(*sqlparser.Select)
	if !isSelect {
		return nil, nil, errors.New("operation is not supported")
	}

	return handleSelect(selectStmt)
}
