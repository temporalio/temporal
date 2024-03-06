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

package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

func TestGrouperNamespaceID_Key(t *testing.T) {
	g := GrouperNamespaceID{}
	k := g.Key(tasks.NewFakeTask(definition.NewWorkflowKey("nid", "", ""), tasks.CategoryTransfer, time.Now()))
	require.Equal(t, "nid", k)
}

func TestGrouperNamespaceID_Predicate(t *testing.T) {
	g := GrouperNamespaceID{}
	p := g.Predicate([]any{"n1", "n2"})
	require.Equal(t, tasks.NewNamespacePredicate([]string{"n1", "n2"}), p)
}

func TestGrouperNamespaceIDAndDestination_Key(t *testing.T) {
	g := GrouperNamespaceIDAndDestination{}
	task := tasks.FakeDestinationTask{
		Task:        tasks.NewFakeTask(definition.NewWorkflowKey("nid", "", ""), tasks.CategoryTransfer, time.Now()),
		Destination: "dest",
	}
	k := g.Key(task)
	require.Equal(t, NamespaceIDAndDestination{"nid", "dest"}, k)
}

func TestGrouperNamespaceIDAndDestination_Predicate(t *testing.T) {
	g := GrouperNamespaceIDAndDestination{}
	p := g.Predicate([]any{NamespaceIDAndDestination{"n1", "d1"}, NamespaceIDAndDestination{"n2", "d2"}})
	expected := predicates.Or(
		predicates.And(
			tasks.NewNamespacePredicate([]string{"n1"}),
			tasks.NewDestinationPredicate([]string{"d1"}),
		),
		predicates.And(
			tasks.NewNamespacePredicate([]string{"n2"}),
			tasks.NewDestinationPredicate([]string{"d2"}),
		),
	)
	require.Equal(t, expected, p)
}
