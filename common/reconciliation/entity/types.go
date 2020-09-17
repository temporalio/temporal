// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package entity

import (
	"errors"
	"fmt"

	"github.com/uber/cadence/common/persistence"
)

type (
	// Execution is a base type for executions which should be checked or fixed.
	Execution struct {
		ShardID    int
		DomainID   string
		WorkflowID string
		RunID      string
		State      int
	}

	// ConcreteExecution is a concrete execution.
	ConcreteExecution struct {
		BranchToken []byte
		TreeID      string
		BranchID    string
		Execution
	}

	// CurrentExecution is a current execution.
	CurrentExecution struct {
		CurrentRunID string
		Execution
	}
)

// ValidateExecution returns an error if Execution is not valid, nil otherwise.
func validateExecution(execution *Execution) error {
	if execution.ShardID < 0 {
		return fmt.Errorf("invalid ShardID: %v", execution.ShardID)
	}
	if len(execution.DomainID) == 0 {
		return errors.New("empty DomainID")
	}
	if len(execution.WorkflowID) == 0 {
		return errors.New("empty WorkflowID")
	}
	if len(execution.RunID) == 0 {
		return errors.New("empty RunID")
	}
	if execution.State < persistence.WorkflowStateCreated || execution.State > persistence.WorkflowStateCorrupted {
		return fmt.Errorf("unknown workflow state: %v", execution.State)
	}
	return nil
}

// Validate returns an error if ConcreteExecution is not valid, nil otherwise.
func (ce *ConcreteExecution) Validate() error {
	err := validateExecution(&ce.Execution)
	if err != nil {
		return err
	}
	if len(ce.BranchToken) == 0 {
		return errors.New("empty BranchToken")
	}
	if len(ce.TreeID) == 0 {
		return errors.New("empty TreeID")
	}
	if len(ce.BranchID) == 0 {
		return errors.New("empty BranchID")
	}
	return nil

}

// Validate returns an error if CurrentExecution is not valid, nil otherwise.
func (curre *CurrentExecution) Validate() error {
	err := validateExecution(&curre.Execution)
	if err != nil {
		return err
	}
	if len(curre.CurrentRunID) == 0 {
		return errors.New("empty CurrentRunID")
	}
	return nil
}

// Clone will return a new copy of ConcreteExecution
func (ConcreteExecution) Clone() Entity {
	return &ConcreteExecution{}
}

// Clone will return a new copy of CurrentExecution
func (CurrentExecution) Clone() Entity {
	return &CurrentExecution{}
}

// Entity allows to deserialize and validate different type of executions
type Entity interface {
	Validate() error
	Clone() Entity
}
