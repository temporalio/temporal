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
	"golang.org/x/exp/maps"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/predicates"
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

func AndPredicates(a Predicate, b Predicate) Predicate {
	switch a := a.(type) {
	case *NamespacePredicate:
		if b, ok := b.(*NamespacePredicate); ok {
			intersection := intersect(a.NamespaceIDs, b.NamespaceIDs)
			if len(intersection) == 0 {
				return predicates.Empty[Task]()
			}
			return &NamespacePredicate{
				NamespaceIDs: intersection,
			}
		}
	case *TypePredicate:
		if b, ok := b.(*TypePredicate); ok {
			intersection := intersect(a.Types, b.Types)
			if len(intersection) == 0 {
				return predicates.Empty[Task]()
			}
			return &TypePredicate{
				Types: intersection,
			}
		}
	}

	return predicates.And(a, b)
}

func OrPredicates(a Predicate, b Predicate) Predicate {
	switch a := a.(type) {
	case *NamespacePredicate:
		if b, ok := b.(*NamespacePredicate); ok {
			return &NamespacePredicate{
				NamespaceIDs: union(a.NamespaceIDs, b.NamespaceIDs),
			}
		}
	case *TypePredicate:
		if b, ok := b.(*TypePredicate); ok {
			return &TypePredicate{
				Types: union(a.Types, b.Types),
			}
		}
	}

	return predicates.Or(a, b)
}

func IsUniverisalPredicate(p Predicate) bool {
	_, ok := p.(*predicates.UniversalImpl[Task])
	return ok
}

func IsNamespacePredicate(p Predicate) bool {
	_, ok := p.(*NamespacePredicate)
	return ok
}

func IsTypePredicate(p Predicate) bool {
	_, ok := p.(*TypePredicate)
	return ok
}

func intersect[K comparable](this, that map[K]struct{}) map[K]struct{} {
	intersection := make(map[K]struct{})
	for key := range this {
		if _, ok := that[key]; ok {
			intersection[key] = struct{}{}
		}
	}
	return intersection
}

func union[K comparable](this, that map[K]struct{}) map[K]struct{} {
	union := make(map[K]struct{}, len(this)+len(that))
	maps.Copy(union, this)
	maps.Copy(union, that)
	return union
}
