package esql

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/xwb1989/sqlparser"
)

// SetTemporal ... specify whether do special handling for temporal visibility
// should not be called if there is potential race condition
// should not be called by non-temporal user
func (e *ESql) SetTemporal(temporalArg bool) {
	e.temporal = temporalArg
}

// ConvertPrettyTemporal ...
// convert sql to es dsl, for temporal usage
func (e *ESql) ConvertPrettyTemporal(sql string, namespaceID string, pagination ...interface{}) (dsl string, sortFields []string, err error) {
	dsl, sortFields, err = e.ConvertTemporal(sql, namespaceID, pagination...)
	if err != nil {
		return "", nil, err
	}

	var prettifiedDSLBytes bytes.Buffer
	err = json.Indent(&prettifiedDSLBytes, []byte(dsl), "", "  ")
	if err != nil {
		return "", nil, err
	}
	return prettifiedDSLBytes.String(), sortFields, err
}

// ConvertTemporal ...
// convert sql to es dsl, for temporal usage
func (e *ESql) ConvertTemporal(sql string, namespaceID string, pagination ...interface{}) (dsl string, sortFields []string, err error) {
	if !e.temporal {
		err = fmt.Errorf(`esql: temporal option not turned on`)
		return "", nil, err
	}
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return "", nil, err
	}

	//sql valid, start to handle
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		dsl, sortFields, err = e.convertSelect(*(stmt), namespaceID, pagination...)
	default:
		err = fmt.Errorf(`esql: Queries other than select not supported`)
	}

	if err != nil {
		return "", nil, err
	}
	return dsl, sortFields, nil
}
