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
	passScope := NewScope(
		s.Range,
		tasks.AndPredicates(s.Predicate, predicate),
	)
	failScope := NewScope(
		s.Range,
		tasks.AndPredicates(s.Predicate, predicates.Not(predicate)),
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

	return NewScope(s.Range, tasks.OrPredicates(s.Predicate, incomingScope.Predicate))
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
