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

package queue

type (
	// ActionType specifies the type of the Action
	ActionType int

	// Action specifies the Action should be performed
	Action struct {
		ActionType            ActionType
		ResetActionAttributes *ResetActionAttributes
		// add attributes for other action types here
	}

	// ActionResult is the result for performing an Action
	ActionResult struct {
		ActionType        ActionType
		ResetActionResult *ResetActionResult
	}

	// ResetActionAttributes contains the parameter for performing Reset Action
	ResetActionAttributes struct{}
	// ResetActionResult is the result for performing Reset Action
	ResetActionResult struct{}

	// TODO: add another Action for reading processing queue state to make
	// the implementation of processorBase lock free
)

const (
	// ActionTypeReset is the ActionType for reseting processing queue states
	ActionTypeReset ActionType = iota + 1
	// add more ActionType here
)

// NewResetAction creates a new action for reseting processing queue states
func NewResetAction() *Action {
	return &Action{
		ActionType:            ActionTypeReset,
		ResetActionAttributes: &ResetActionAttributes{},
	}
}
