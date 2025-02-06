// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package transitionhistory

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
)

func CopyVersionedTransitions(
	transitions []*persistencespb.VersionedTransition,
) []*persistencespb.VersionedTransition {
	if transitions == nil {
		return nil
	}
	copied := make([]*persistencespb.VersionedTransition, len(transitions))
	for i, t := range transitions {
		copied[i] = CopyVersionedTransition(t)
	}
	return copied
}

func CopyVersionedTransition(
	transition *persistencespb.VersionedTransition,
) *persistencespb.VersionedTransition {
	if transition == nil {
		return nil
	}
	return common.CloneProto(transition)
}

func LastVersionedTransition(
	transitions []*persistencespb.VersionedTransition,
) *persistencespb.VersionedTransition {
	if len(transitions) == 0 {
		// transition history is not enabled
		return nil
	}
	return transitions[len(transitions)-1]
}
