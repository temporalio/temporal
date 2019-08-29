// Copyright (c) 2019 Uber Technologies, Inc.
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

package testing

type (
	// NDCTestBatch is the struct for history event batch
	NDCTestBatch struct {
		Events []Vertex
	}

	// NDCTestBranch is a branch of new history events
	NDCTestBranch struct {
		Next    []*NDCTestBranch
		Batches []NDCTestBatch
	}
)

// Split creates a new branch of the workflow execution
func (b *NDCTestBranch) Split(resetIdx int) *NDCTestBranch {
	curr := getBranchToSplit(b, resetIdx)
	return updateCurrentBranchWithSplit(curr, resetIdx)
}

func getBranchToSplit(root *NDCTestBranch, resetIdx int) *NDCTestBranch {
	curr := root
	for curr.Next != nil {
		length := len(curr.Batches)
		if length > resetIdx {
			break
		}
		curr = curr.Next[len(curr.Next)-1]
		resetIdx -= length
	}
	return curr
}

func updateCurrentBranchWithSplit(curr *NDCTestBranch, resetIdx int) *NDCTestBranch {
	firstBatches := make([]NDCTestBatch, resetIdx+1)
	copy(firstBatches, curr.Batches[:resetIdx+1])
	secondBatches := make([]NDCTestBatch, len(curr.Batches)-resetIdx-1)
	copy(secondBatches, curr.Batches[resetIdx+1:])
	splitBranch := &NDCTestBranch{
		Next:    curr.Next,
		Batches: secondBatches,
	}
	newBranch := &NDCTestBranch{
		Batches: make([]NDCTestBatch, 0),
	}
	curr.Batches = firstBatches
	curr.Next = []*NDCTestBranch{splitBranch, newBranch}
	return newBranch
}
