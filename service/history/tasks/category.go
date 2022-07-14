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
	"strconv"
	"sync"

	"golang.org/x/exp/maps"

	enumsspb "go.temporal.io/server/api/enums/v1"
)

type (
	Category struct {
		id    int32
		cType CategoryType
		name  string
	}

	CategoryType int
)

const (
	CategoryIDUnspecified = int32(enumsspb.TASK_CATEGORY_UNSPECIFIED)
	CategoryIDTransfer    = int32(enumsspb.TASK_CATEGORY_TRANSFER)
	CategoryIDTimer       = int32(enumsspb.TASK_CATEGORY_TIMER)
	CategoryIDReplication = int32(enumsspb.TASK_CATEGORY_REPLICATION)
	CategoryIDVisibility  = int32(enumsspb.TASK_CATEGORY_VISIBILITY)
)

const (
	CategoryTypeUnspecified CategoryType = iota
	CategoryTypeImmediate
	CategoryTypeScheduled
)

const (
	CategoryNameTransfer    = "transfer"
	CategoryNameTimer       = "timer"
	CategoryNameReplication = "replication"
	CategoryNameVisibility  = "visibility"
)

var (
	CategoryTransfer = Category{
		id:    CategoryIDTransfer,
		cType: CategoryTypeImmediate,
		name:  CategoryNameTransfer,
	}

	CategoryTimer = Category{
		id:    CategoryIDTimer,
		cType: CategoryTypeScheduled,
		name:  CategoryNameTimer,
	}

	CategoryReplication = Category{
		id:    CategoryIDReplication,
		cType: CategoryTypeImmediate,
		name:  CategoryNameReplication,
	}

	CategoryVisibility = Category{
		id:    CategoryIDVisibility,
		cType: CategoryTypeImmediate,
		name:  CategoryNameVisibility,
	}
)

var (
	categories = struct {
		sync.RWMutex
		m map[int32]Category
	}{
		m: map[int32]Category{
			CategoryTransfer.ID():    CategoryTransfer,
			CategoryTimer.ID():       CategoryTimer,
			CategoryVisibility.ID():  CategoryVisibility,
			CategoryReplication.ID(): CategoryReplication,
		},
	}
)

// NewCategory creates a new Category and register the created Category
// Registered Categories can be retrieved via GetCategories() or GetCategoryByID()
// NewCategory panics when a Category with the same ID has already been registered
func NewCategory(
	id int32,
	categoryType CategoryType,
	name string,
) Category {
	categories.Lock()
	defer categories.Unlock()

	if category, ok := categories.m[id]; ok {
		panic(fmt.Sprintf("category id: %v has already been defined as type %v and name %v", id, category.cType, category.name))
	}

	newCategory := Category{
		id:    id,
		cType: categoryType,
		name:  name,
	}
	categories.m[id] = newCategory

	return newCategory
}

// GetCategories returns a deep copy of all registered Categories
func GetCategories() map[int32]Category {
	categories.RLock()
	defer categories.RUnlock()
	return maps.Clone(categories.m)
}

// GetCategoryByID returns a registered Category with the same ID
func GetCategoryByID(id int32) (Category, bool) {
	categories.RLock()
	defer categories.RUnlock()

	category, ok := categories.m[id]
	return category, ok
}

func (c *Category) ID() int32 {
	return c.id
}

func (c *Category) Name() string {
	return c.name
}

func (c *Category) Type() CategoryType {
	return c.cType
}

func (t CategoryType) String() string {
	switch t {
	case CategoryTypeImmediate:
		return "Immediate"
	case CategoryTypeScheduled:
		return "Scheduled"
	default:
		return strconv.Itoa(int(t))
	}
}
