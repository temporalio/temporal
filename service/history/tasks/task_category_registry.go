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
	"fmt"

	"golang.org/x/exp/maps"
)

type (
	// TaskCategoryRegistry keeps track of all registered Category objects.
	TaskCategoryRegistry interface {
		GetCategoryByID(id int) (Category, bool)
		GetCategories() map[int]Category
	}
	// MutableTaskCategoryRegistry is a TaskCategoryRegistry which also supports the addition of new categories.
	// We separate the two types to prevent the addition of new categories after the initialization phase; only the
	// immutable TaskCategoryRegistry should be available after initialization. This object is not thread-safe, but it
	// is intended to be used with fx, which is single-threaded.
	MutableTaskCategoryRegistry struct {
		categories map[int]Category
	}
)

// NewDefaultTaskCategoryRegistry creates a new MutableTaskCategoryRegistry with the default categories. The "default"
// categories here are really the unconditional categories: task categories that we always process, independent of any
// configuration. This type is intended to be used as a singleton, so you should only create a new instance once for
// each entry point that uses it. Essentially, get it from the dependency graph instead of calling this method, unless
// you're in a test.
func NewDefaultTaskCategoryRegistry() *MutableTaskCategoryRegistry {
	return &MutableTaskCategoryRegistry{
		categories: map[int]Category{
			CategoryTransfer.ID():    CategoryTransfer,
			CategoryTimer.ID():       CategoryTimer,
			CategoryVisibility.ID():  CategoryVisibility,
			CategoryReplication.ID(): CategoryReplication,
			CategoryMemoryTimer.ID(): CategoryMemoryTimer,
		},
	}
}

// AddCategory register a Category with the registry or panics if a Category with the same ID has already been
// registered.
func (r *MutableTaskCategoryRegistry) AddCategory(c Category) {
	if category, ok := r.categories[c.id]; ok {
		panic(fmt.Sprintf(
			"category id: %v has already been defined as type %v and name %v",
			c.id,
			category.cType,
			category.name,
		))
	}

	r.categories[c.id] = c
}

// GetCategoryByID returns a registered Category with the same ID from the registry or false if no such Category exists.
func (r *MutableTaskCategoryRegistry) GetCategoryByID(id int) (Category, bool) {
	category, ok := r.categories[id]
	return category, ok
}

// GetCategories returns a deep copy of all registered Category objects from the registry.
func (r *MutableTaskCategoryRegistry) GetCategories() map[int]Category {
	return maps.Clone(r.categories)
}
