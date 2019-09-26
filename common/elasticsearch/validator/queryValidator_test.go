// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to qvom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, qvETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package validator

import (
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"testing"
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

	listRequest := &shared.ListWorkflowExecutionsRequest{}
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal("", listRequest.GetQuery())

	query := "WorkflowID = 'wid'"
	listRequest.Query = common.StringPtr(query)
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal(query, listRequest.GetQuery())

	query = "CustomStringField = 'custom'"
	listRequest.Query = common.StringPtr(query)
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal("`Attr.CustomStringField` = 'custom'", listRequest.GetQuery())

	query = "WorkflowID = 'wid' and ((CustomStringField = 'custom') or CustomIntField between 1 and 10)"
	listRequest.Query = common.StringPtr(query)
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal("WorkflowID = 'wid' and ((`Attr.CustomStringField` = 'custom') or `Attr.CustomIntField` between 1 and 10)", listRequest.GetQuery())

	query = "Invalid SQL"
	listRequest.Query = common.StringPtr(query)
	s.Equal("BadRequestError{Message: Invalid query.}", qv.ValidateListRequestForQuery(listRequest).Error())

	query = "InvalidWhereExpr"
	listRequest.Query = common.StringPtr(query)
	s.Equal("BadRequestError{Message: invalid where clause}", qv.ValidateListRequestForQuery(listRequest).Error())

	// Invalid comparison
	query = "WorkflowID = 'wid' and 1 < 2"
	listRequest.Query = common.StringPtr(query)
	s.Equal("BadRequestError{Message: invalid comparison expression}", qv.ValidateListRequestForQuery(listRequest).Error())

	// Invalid range
	query = "1 between 1 and 2 or WorkflowID = 'wid'"
	listRequest.Query = common.StringPtr(query)
	s.Equal("BadRequestError{Message: invalid range expression}", qv.ValidateListRequestForQuery(listRequest).Error())

	// Invalid search attribute in comparison
	query = "Invalid = 'a' and 1 < 2"
	listRequest.Query = common.StringPtr(query)
	s.Equal("BadRequestError{Message: invalid search attribute}", qv.ValidateListRequestForQuery(listRequest).Error())

	// Invalid search attribute in range
	query = "Invalid between 1 and 2 or WorkflowID = 'wid'"
	listRequest.Query = common.StringPtr(query)
	s.Equal("BadRequestError{Message: invalid search attribute}", qv.ValidateListRequestForQuery(listRequest).Error())

	// only order by
	query = "order by CloseTime desc"
	listRequest.Query = common.StringPtr(query)
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal(" "+query, listRequest.GetQuery())

	// only order by search attribute
	query = "order by CustomIntField desc"
	listRequest.Query = common.StringPtr(query)
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal(" order by `Attr.CustomIntField` desc", listRequest.GetQuery())

	// condition + order by
	query = "WorkflowID = 'wid' order by CloseTime desc"
	listRequest.Query = common.StringPtr(query)
	s.Nil(qv.ValidateListRequestForQuery(listRequest))
	s.Equal(query, listRequest.GetQuery())

	// invalid order by attribute
	query = "order by InvalidField desc"
	listRequest.Query = common.StringPtr(query)
	s.Equal("BadRequestError{Message: invalid order by attribute}", qv.ValidateListRequestForQuery(listRequest).Error())

	// invalid order by attribute expr
	query = "order by 123"
	listRequest.Query = common.StringPtr(query)
	s.Equal("BadRequestError{Message: invalid order by expression}", qv.ValidateListRequestForQuery(listRequest).Error())

	// security SQL injection
	query = "WorkflowID = 'wid'; SELECT * FROM important_table;"
	listRequest.Query = common.StringPtr(query)
	s.Equal("BadRequestError{Message: Invalid query.}", qv.ValidateListRequestForQuery(listRequest).Error())

	query = "WorkflowID = 'wid' and (RunID = 'rid' or 1 = 1)"
	listRequest.Query = common.StringPtr(query)
	s.NotNil(qv.ValidateListRequestForQuery(listRequest))

	query = "WorkflowID = 'wid' union select * from dummy"
	listRequest.Query = common.StringPtr(query)
	s.NotNil(qv.ValidateListRequestForQuery(listRequest))
}
