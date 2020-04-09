package validator

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/temporal-proto/workflowservice"

	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type queryValidatorSuite struct {
	suite.Suite
}

func TestQueryValidatorSuite(t *testing.T) {
	s := new(queryValidatorSuite)
	suite.Run(t, s)
}

func (s *queryValidatorSuite) TestValidateListRequestForQuery() {
	validSearchAttr := dynamicconfig.GetMapPropertyFn(definition.GetDefaultIndexedKeys())
	qv := NewQueryValidator(validSearchAttr)

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{}
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal("", listRequest.GetQuery())

	query := "WorkflowId = 'wid'"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal(query, listRequest.GetQuery())

	query = "CustomStringField = 'custom'"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal("`Attr.CustomStringField` = 'custom'", listRequest.GetQuery())

	query = "WorkflowId = 'wid' and ((CustomStringField = 'custom') or CustomIntField between 1 and 10)"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal("WorkflowId = 'wid' and ((`Attr.CustomStringField` = 'custom') or `Attr.CustomIntField` between 1 and 10)", listRequest.GetQuery())

	query = "Invalid SQL"
	listRequest.Query = query
	s.Equal("Invalid query.", qv.ValidateListRequestForQuery(listRequest).Error())

	query = "InvalidWhereExpr"
	listRequest.Query = query
	s.Equal("invalid where clause", qv.ValidateListRequestForQuery(listRequest).Error())

	// Invalid comparison
	query = "WorkflowId = 'wid' and 1 < 2"
	listRequest.Query = query
	s.Equal("invalid comparison expression", qv.ValidateListRequestForQuery(listRequest).Error())

	// Invalid range
	query = "1 between 1 and 2 or WorkflowId = 'wid'"
	listRequest.Query = query
	s.Equal("invalid range expression", qv.ValidateListRequestForQuery(listRequest).Error())

	// Invalid search attribute in comparison
	query = "Invalid = 'a' and 1 < 2"
	listRequest.Query = query
	s.Equal("invalid search attribute", qv.ValidateListRequestForQuery(listRequest).Error())

	// Invalid search attribute in range
	query = "Invalid between 1 and 2 or WorkflowId = 'wid'"
	listRequest.Query = query
	s.Equal("invalid search attribute", qv.ValidateListRequestForQuery(listRequest).Error())

	// only order by
	query = "order by CloseTime desc"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal(" "+query, listRequest.GetQuery())

	// only order by search attribute
	query = "order by CustomIntField desc"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal(" order by `Attr.CustomIntField` desc", listRequest.GetQuery())

	// condition + order by
	query = "WorkflowId = 'wid' order by CloseTime desc"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal(query, listRequest.GetQuery())

	// invalid order by attribute
	query = "order by InvalidField desc"
	listRequest.Query = query
	s.Equal("invalid order by attribute", qv.ValidateListRequestForQuery(listRequest).Error())

	// invalid order by attribute expr
	query = "order by 123"
	listRequest.Query = query
	s.Equal("invalid order by expression", qv.ValidateListRequestForQuery(listRequest).Error())

	// security SQL injection
	query = "WorkflowId = 'wid'; SELECT * FROM important_table;"
	listRequest.Query = query
	s.Equal("Invalid query.", qv.ValidateListRequestForQuery(listRequest).Error())

	query = "WorkflowId = 'wid' and (RunId = 'rid' or 1 = 1)"
	listRequest.Query = query
	s.NotNil(qv.ValidateListRequestForQuery(listRequest))

	query = "WorkflowId = 'wid' union select * from dummy"
	listRequest.Query = query
	s.NotNil(qv.ValidateListRequestForQuery(listRequest))
}
