// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	"testing"

	"github.com/stretchr/testify/suite"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type (
	decisionAttrValidatorSuite struct {
		suite.Suite

		maxIDLengthLimit int
		validator        *decisionAttrValidator
	}
)

func TestDecisionAttrValidatorSuite(t *testing.T) {
	s := new(decisionAttrValidatorSuite)
	suite.Run(t, s)
}

func (s *decisionAttrValidatorSuite) SetupSuite() {
}

func (s *decisionAttrValidatorSuite) TearDownSuite() {

}

func (s *decisionAttrValidatorSuite) SetupTest() {
	s.maxIDLengthLimit = 1000
	s.validator = newDecisionAttrValidator(s.maxIDLengthLimit)
}

func (s *decisionAttrValidatorSuite) TearDownTest() {

}

func (s *decisionAttrValidatorSuite) TestValidateSignalExternalWorkflowExecutionAttributes() {
	var attributes *workflow.SignalExternalWorkflowExecutionDecisionAttributes

	err := s.validator.validateSignalExternalWorkflowExecutionAttributes(attributes)
	s.EqualError(err, "BadRequestError{Message: SignalExternalWorkflowExecutionDecisionAttributes is not set on decision.}")

	attributes = &workflow.SignalExternalWorkflowExecutionDecisionAttributes{}
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(attributes)
	s.EqualError(err, "BadRequestError{Message: Execution is nil on decision.}")

	attributes.Execution = &workflow.WorkflowExecution{}
	attributes.Execution.WorkflowId = common.StringPtr("workflow-id")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(attributes)
	s.EqualError(err, "BadRequestError{Message: SignalName is not set on decision.}")

	attributes.Execution.RunId = common.StringPtr("run-id")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(attributes)
	s.EqualError(err, "BadRequestError{Message: Invalid RunId set on decision.}")
	attributes.Execution.RunId = common.StringPtr(validRunID)

	attributes.SignalName = common.StringPtr("my signal name")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(attributes)
	s.EqualError(err, "BadRequestError{Message: Input is not set on decision.}")

	attributes.Input = []byte("test input")
	err = s.validator.validateSignalExternalWorkflowExecutionAttributes(attributes)
	s.Nil(err)
}
