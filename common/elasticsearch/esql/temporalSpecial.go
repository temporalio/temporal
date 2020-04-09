package esql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func (e *ESql) addTemporalSort(orderBySlice []string, sortFields []string) ([]string, []string, error) {
	switch len(orderBySlice) {
	case 0: // if unsorted, use default sorting
		temporalOrderStartTime := fmt.Sprintf(`{"%v": "%v"}`, StartTime, StartTimeOrder)
		orderBySlice = append(orderBySlice, temporalOrderStartTime)
		sortFields = append(sortFields, StartTime)
	case 1: // user should not use tieBreaker to sort
		if sortFields[0] == TieBreaker {
			err := fmt.Errorf("esql: Temporal does not allow user sort by RunId")
			return nil, nil, err
		}
	default:
		err := fmt.Errorf("esql: Temporal only allow 1 custom sort field")
		return nil, nil, err
	}

	// add tie breaker
	temporalOrderTieBreaker := fmt.Sprintf(`{"%v": "%v"}`, TieBreaker, TieBreakerOrder)
	orderBySlice = append(orderBySlice, temporalOrderTieBreaker)
	sortFields = append(sortFields, TieBreaker)
	return orderBySlice, sortFields, nil
}

func (e *ESql) addTemporalNamespaceTimeQuery(sel sqlparser.Select, namespaceID string, dslMap map[string]interface{}) {
	var namespaceIDQuery string
	if namespaceID != "" {
		namespaceIDQuery = fmt.Sprintf(`{"term": {"%v": "%v"}}`, NamespaceID, namespaceID)
	}
	if sel.Where == nil {
		if namespaceID != "" {
			dslMap["query"] = namespaceIDQuery
		}
	} else {
		if namespaceID != "" {
			namespaceIDQuery = namespaceIDQuery + ","
		}
		if strings.Contains(fmt.Sprintf("%v", dslMap["query"]), ExecutionTime) {
			executionTimeBound := fmt.Sprintf(`{"range": {"%v": {"gte": "0"}}}`, ExecutionTime)
			dslMap["query"] = fmt.Sprintf(`{"bool": {"filter": [%v %v, %v]}}`, namespaceIDQuery, executionTimeBound, dslMap["query"])
		} else {
			dslMap["query"] = fmt.Sprintf(`{"bool": {"filter": [%v %v]}}`, namespaceIDQuery, dslMap["query"])
		}
	}
}
