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
	CategoryIDUnspecified int32 = iota
	CategoryIDTransfer
	CategoryIDTimer
	CategoryIDVisibility
	CategoryIDReplication
)

const (
	CategoryTypeUnspecified CategoryType = iota
	CategoryTypeImmediate
	CategoryTypeScheduled
)

const (
	CategoryNameTransfer    = "transfer"
	CategoryNameTimer       = "timer"
	CategoryNameVisibility  = "visibility"
	CategoryNameReplication = "replication"
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

	CategoryVisibility = Category{
		id:    CategoryIDVisibility,
		cType: CategoryTypeImmediate,
		name:  CategoryNameVisibility,
	}

	CategoryReplication = Category{
		id:    CategoryIDReplication,
		cType: CategoryTypeImmediate,
		name:  CategoryNameReplication,
	}
)

var (
	categories = struct {
		sync.Mutex
		list []Category
	}{
		list: []Category{
			CategoryTransfer,
			CategoryTimer,
			CategoryVisibility,
			CategoryReplication,
		},
	}
)

func NewCategory(
	id int32,
	categoryType CategoryType,
	name string,
) Category {
	categories.Lock()
	defer categories.Unlock()

	isNewCategory := true
	for _, existingCategory := range categories.list {
		if existingCategory.ID() == id {
			isNewCategory = false
			if existingCategory.cType != categoryType {
				panic(fmt.Sprintf("category id: %v has already been defined as type: %v ", id, existingCategory.cType))
			}
		}
	}

	newCategory := Category{
		id:    id,
		cType: categoryType,
		name:  name,
	}
	if isNewCategory {
		categories.list = append(categories.list, newCategory)
	}

	return newCategory
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
