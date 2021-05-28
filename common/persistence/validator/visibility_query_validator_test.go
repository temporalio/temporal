// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package validator

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/searchattribute"
)

type queryValidatorSuite struct {
	suite.Suite
	*require.Assertions

	controller *gomock.Controller
}

func TestQueryValidatorSuite(t *testing.T) {
	suite.Run(t, &queryValidatorSuite{})
}

func (s *queryValidatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
}

func (s *queryValidatorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *queryValidatorSuite) TestValidateListRequestForQuery() {
	searchAttributesProvider := searchattribute.NewMockProvider(s.controller)
	searchAttributesProvider.EXPECT().GetSearchAttributes("index-name", false).
		Return(searchattribute.TestNameTypeMap, nil).
		AnyTimes()

	qv := NewQueryValidator(searchAttributesProvider)

	listRequest := &workflowservice.ListWorkflowExecutionsRequest{}
	s.Nil(qv.ValidateListRequestForQuery(listRequest, "index-name"))
	s.Equal("", listRequest.GetQuery())

	query := "WorkflowId = 'wid'"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest, "index-name"))
	s.Equal(query, listRequest.GetQuery())

	query = "CustomStringField = 'custom'"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest, "index-name"))
	s.Equal("CustomStringField = 'custom'", listRequest.GetQuery())

	query = "WorkflowId = 'wid' and ((CustomStringField = 'custom') or CustomIntField between 1 and 10)"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest, "index-name"))
	s.Equal("WorkflowId = 'wid' and ((CustomStringField = 'custom') or CustomIntField between 1 and 10)", listRequest.GetQuery())

	query = "Invalid SQL"
	listRequest.Query = query
	s.Equal("Invalid query.", qv.ValidateListRequestForQuery(listRequest, "index-name").Error())

	query = "InvalidWhereExpr"
	listRequest.Query = query
	s.Equal("invalid where clause", qv.ValidateListRequestForQuery(listRequest, "index-name").Error())

	// Invalid comparison
	query = "WorkflowId = 'wid' and 1 < 2"
	listRequest.Query = query
	s.Equal("invalid comparison expression", qv.ValidateListRequestForQuery(listRequest, "index-name").Error())

	// Invalid range
	query = "1 between 1 and 2 or WorkflowId = 'wid'"
	listRequest.Query = query
	s.Equal("invalid range expression", qv.ValidateListRequestForQuery(listRequest, "index-name").Error())

	// Invalid search attribute in comparison
	query = "Invalid = 'a' and 1 < 2"
	listRequest.Query = query
	s.Equal("invalid search attribute: Invalid", qv.ValidateListRequestForQuery(listRequest, "index-name").Error())

	// Invalid search attribute in range
	query = "Invalid between 1 and 2 or WorkflowId = 'wid'"
	listRequest.Query = query
	s.Equal("invalid search attribute: Invalid", qv.ValidateListRequestForQuery(listRequest, "index-name").Error())

	// only order by
	query = "order by CloseTime desc"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest, "index-name"))
	s.Equal(" "+query, listRequest.GetQuery())

	// only order by search attribute
	query = "order by CustomIntField desc"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest, "index-name"))
	s.Equal(" order by CustomIntField desc", listRequest.GetQuery())

	// condition + order by
	query = "WorkflowId = 'wid' order by CloseTime desc"
	listRequest.Query = query
	s.Nil(qv.ValidateListRequestForQuery(listRequest, "index-name"))
	s.Equal(query, listRequest.GetQuery())

	// invalid order by attribute
	query = "order by InvalidField desc"
	listRequest.Query = query
	s.Equal("invalid order by attribute: InvalidField", qv.ValidateListRequestForQuery(listRequest, "index-name").Error())

	// invalid order by attribute expr
	query = "order by 123"
	listRequest.Query = query
	s.Equal("invalid order by expression", qv.ValidateListRequestForQuery(listRequest, "index-name").Error())

	// security SQL injection
	query = "WorkflowId = 'wid'; SELECT * FROM important_table;"
	listRequest.Query = query
	s.Equal("Invalid query.", qv.ValidateListRequestForQuery(listRequest, "index-name").Error())

	query = "WorkflowId = 'wid' and (RunId = 'rid' or 1 = 1)"
	listRequest.Query = query
	s.NotNil(qv.ValidateListRequestForQuery(listRequest, "index-name"))

	query = "WorkflowId = 'wid' union select * from dummy"
	listRequest.Query = query
	s.NotNil(qv.ValidateListRequestForQuery(listRequest, "index-name"))
}
func (s *queryValidatorSuite) TestValidateListRequestForQuery_NoCustomSearchAttributes() {
	searchAttributesProvider := searchattribute.NewMockProvider(s.controller)
	searchAttributesProvider.EXPECT().GetSearchAttributes("index-name", false).
		Return(searchattribute.NameTypeMap{}, nil).
		AnyTimes()

	qv := NewQueryValidator(searchAttributesProvider)

	// system search attributes should pass through.
	listRequest := &workflowservice.ListWorkflowExecutionsRequest{}
	query := "WorkflowId = 'wid'"
	listRequest.Query = query
	s.NoError(qv.ValidateListRequestForQuery(listRequest, "index-name"))
	s.Equal("WorkflowId = 'wid'", listRequest.GetQuery())

	// Custom search attributes should fail.
	query = "CustomStringField = 'wid'"
	listRequest.Query = query
	s.Error(qv.ValidateListRequestForQuery(listRequest, "index-name"))
}
