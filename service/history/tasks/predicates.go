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

package tasks

import (
	"go.temporal.io/server/common/predicates"
	"golang.org/x/exp/maps"

	enumsspb "go.temporal.io/server/api/enums/v1"
)

type (
	Predicate = predicates.Predicate[Task]
)

var (
	_ Predicate = (*NamespacePredicate)(nil)
	_ Predicate = (*TypePredicate)(nil)
)

type (
	NamespacePredicate struct {
		NamespaceIDs map[string]struct{}
	}

	TypePredicate struct {
		Types map[enumsspb.TaskType]struct{}
	}
)

func NewNamespacePredicate(
	namespaceIDs []string,
) *NamespacePredicate {
	namespaceIDMap := make(map[string]struct{}, len(namespaceIDs))
	for _, id := range namespaceIDs {
		namespaceIDMap[id] = struct{}{}
	}

	return &NamespacePredicate{
		NamespaceIDs: namespaceIDMap,
	}
}

func (n *NamespacePredicate) Test(task Task) bool {
	_, ok := n.NamespaceIDs[task.GetNamespaceID()]
	return ok
}

func (n *NamespacePredicate) Equals(predicate Predicate) bool {
	nsPrediate, ok := predicate.(*NamespacePredicate)
	if !ok {
		return false
	}

	return maps.Equal(n.NamespaceIDs, nsPrediate.NamespaceIDs)
}

func NewTypePredicate(
	types []enumsspb.TaskType,
) *TypePredicate {
	typeMap := make(map[enumsspb.TaskType]struct{}, len(types))
	for _, taskType := range types {
		typeMap[taskType] = struct{}{}
	}

	return &TypePredicate{
		Types: typeMap,
	}
}

func (t *TypePredicate) Test(task Task) bool {
	_, ok := t.Types[task.GetType()]
	return ok
}

func (t *TypePredicate) Equals(predicate Predicate) bool {
	typePrediate, ok := predicate.(*TypePredicate)
	if !ok {
		return false
	}

	return maps.Equal(t.Types, typePrediate.Types)
}
