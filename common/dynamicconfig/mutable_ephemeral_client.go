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

package dynamicconfig

import (
	"strings"
	"sync"

	"golang.org/x/exp/slices"

	"go.temporal.io/server/common/log"
)

// MutableEphemeralClient is a dynamicconfig.Client implementation that is
// safely mutable in the presence of an unbounded number of reads and writes.
// Writers block other writers but not readers. Readers don't block anything.
// Mutations are batched to the Update function. Data written to objects of this
// type is not made durable.
type MutableEphemeralClient struct {
	*basicClient
	mu sync.Mutex
}

// NewMutableEphemeralClient constructs a new MutableEphemeralClient with an
// empty internal store.
func NewMutableEphemeralClient(mutations ...Mutation) *MutableEphemeralClient {
	c := &MutableEphemeralClient{basicClient: newBasicClient(log.NewNoopLogger())}
	c.Update(mutations...)
	return c
}

// MutationConstraint expresses a configuration constraint (namespace, queue
// name, or queue type) that is an optional part of a Mutation.
type MutationConstraint func(map[string]interface{})

// Mutation expresses a modification to a MutableEphemeralClient's internal
// value store. Mutation functions are applied with
// MutableEphemeralClient.Update.
type Mutation func(configValueMap)

// ForNamespaces builds an exact-match namespace MutationConstraint.
func ForNamespace(ns string) MutationConstraint {
	return func(m map[string]interface{}) {
		m[Namespace.String()] = ns
	}
}

// ForTaskQueueName builds an exact-match task queue name MutationConstraint.
func ForTaskQueueName(n string) MutationConstraint {
	return func(m map[string]interface{}) {
		m[TaskQueueName.String()] = n
	}
}

// ForTaskType builds an exact-match task type MutationConstraint.
func ForTaskType(t int) MutationConstraint {
	return func(m map[string]interface{}) {
		m[TaskType.String()] = t
	}
}

// Set assigns the supplied value along with optional constraints to the
// indicated key. Any other additional value (possibly with other constraints)
// is overwritten.
func Set(k Key, v interface{}, constrainers ...MutationConstraint) Mutation {
	return func(m configValueMap) {
		kstr := strings.ToLower(k.String())
		cv := constrainedValue{Value: v, Constraints: map[string]interface{}{}}
		for _, cf := range constrainers {
			cf(cv.Constraints)
		}
		m[kstr] = []*constrainedValue{&cv}
	}
}

// Add appends the supplied value and optional constraints to those already
// present under the indicated key. If there are no values present for the key
// this is equivalent to Set.
func Add(k Key, v interface{}, constrainers ...MutationConstraint) Mutation {
	return func(m configValueMap) {
		kstr := strings.ToLower(k.String())
		cv := constrainedValue{Value: v, Constraints: map[string]interface{}{}}
		for _, cf := range constrainers {
			cf(cv.Constraints)
		}
		m[kstr] = append(m[kstr], &cv)
	}
}

// Updated applies the provided Mutations to this MutableEphemeralClients
// underlying non-durable value store.
func (c *MutableEphemeralClient) Update(mutations ...Mutation) {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.values.Load().(configValueMap)
	newvals := make(configValueMap, len(current))
	for k, v := range current {
		newvals[k] = slices.Clone(v)
	}
	for _, mutate := range mutations {
		mutate(newvals)
	}
	c.values.Store(newvals)
}

// Set assigns a single configuration value, overwriting any existing values.
func (c *MutableEphemeralClient) Set(
	k Key,
	v interface{},
	mcs ...MutationConstraint,
) {
	c.Update(Set(k, v, mcs...))
}

// Set adds a value to the set of configured values for a given key.
func (c *MutableEphemeralClient) Add(
	k Key,
	v interface{},
	mcs ...MutationConstraint,
) {
	c.Update(Add(k, v, mcs...))
}

// MSet assigns to multiple keys atomically. The supplied MutationConstraints
// are applied to each of the resulting configuration values.
func (c *MutableEphemeralClient) MSet(
	kvs map[Key]interface{},
	mcs ...MutationConstraint,
) {
	c.multi(Set, kvs, mcs...)
}

// MSet appends to multiple keys atomically. The supplied MutationConstraints
// are applied to each of the resulting configuration values.
func (c *MutableEphemeralClient) MAdd(
	kvs map[Key]interface{},
	mcs ...MutationConstraint,
) {
	c.multi(Add, kvs, mcs...)
}

func (c *MutableEphemeralClient) multi(
	f func(Key, interface{}, ...MutationConstraint) Mutation,
	kvs map[Key]interface{},
	mcs ...MutationConstraint,
) {
	mutations := make([]Mutation, 0, len(kvs))
	for k, v := range kvs {
		mutations = append(mutations, f(k, v, mcs...))
	}
	c.Update(mutations...)
}
