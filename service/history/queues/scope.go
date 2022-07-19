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

package queues

import (
	"fmt"

	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

type (
	Scope struct {
		Range     Range
		Predicate tasks.Predicate
	}
)

func NewScope(
	r Range,
	predicate tasks.Predicate,
) Scope {
	return Scope{
		Range:     r,
		Predicate: predicate,
	}
}

func (s *Scope) Contains(task tasks.Task) bool {
	return s.Range.ContainsKey(task.GetKey()) &&
		s.Predicate.Test(task)
}

func (s *Scope) CanSplitByRange(
	key tasks.Key,
) bool {
	return s.Range.CanSplit(key)
}

func (s *Scope) SplitByRange(
	key tasks.Key,
) (left Scope, right Scope) {
	if !s.CanSplitByRange(key) {
		panic(fmt.Sprintf("Unable to split scope with range %v at %v", s.Range, key))
	}

	leftRange, rightRange := s.Range.Split(key)
	return NewScope(leftRange, s.Predicate), NewScope(rightRange, s.Predicate)
}

func (s *Scope) SplitByPredicate(
	predicate tasks.Predicate,
) (pass Scope, fail Scope) {
	// TODO: special check if the predicates are the same type
	passScope := NewScope(
		s.Range,
		predicates.And(s.Predicate, predicate),
	)
	failScope := NewScope(
		s.Range,
		predicates.And(
			s.Predicate,
			predicates.Not(predicate),
		),
	)
	return passScope, failScope
}

func (s *Scope) CanMergeByRange(
	incomingScope Scope,
) bool {
	return s.Range.CanMerge(incomingScope.Range) &&
		s.Predicate.Equals(incomingScope.Predicate)
}

func (s *Scope) MergeByRange(
	incomingScope Scope,
) Scope {
	if !s.CanMergeByRange(incomingScope) {
		panic(fmt.Sprintf("Unable to merge scope with range %v with range %v by range", s.Range, incomingScope.Range))
	}

	return NewScope(s.Range.Merge(incomingScope.Range), s.Predicate)
}

func (s *Scope) CanMergeByPredicate(
	incomingScope Scope,
) bool {
	return s.Range.Equals(incomingScope.Range)
}

func (s *Scope) MergeByPredicate(
	incomingScope Scope,
) Scope {
	if !s.CanMergeByPredicate(incomingScope) {
		panic(fmt.Sprintf("Unable to merge scope with range %v with range %v by predicate", s.Range, incomingScope.Range))
	}

	// TODO: special check if the predicates are the same type
	return NewScope(s.Range, predicates.Or(s.Predicate, incomingScope.Predicate))
}

func (s *Scope) IsEmpty() bool {
	if s.Range.IsEmpty() {
		return true
	}

	if _, ok := s.Predicate.(*predicates.EmptyImpl[tasks.Task]); ok {
		return true
	}

	return false
}

func (s *Scope) Equals(scope Scope) bool {
	return s.Range.Equals(scope.Range) &&
		s.Predicate.Equals(scope.Predicate)
}
