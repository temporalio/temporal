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
	"strconv"
)

type (
	Category struct {
		id    int
		cType CategoryType
		name  string
	}

	CategoryType int
)

// WARNING: These IDS are persisted in the database. Do not change them.

const (
	CategoryIDTransfer    = 1
	CategoryIDTimer       = 2
	CategoryIDReplication = 3
	CategoryIDVisibility  = 4
	CategoryIDArchival    = 5
	CategoryIDMemoryTimer = 6
	CategoryIDOutbound    = 7
)

const (
	_ CategoryType = iota // Reserved for unknown type
	CategoryTypeImmediate
	CategoryTypeScheduled
)

const (
	categoryNameTransfer    = "transfer"
	categoryNameTimer       = "timer"
	categoryNameReplication = "replication"
	categoryNameVisibility  = "visibility"
	categoryNameArchival    = "archival"
	categoryNameMemoryTimer = "memory-timer"
	categoryNameOutbound    = "outbound"
)

var (
	CategoryTransfer = Category{
		id:    CategoryIDTransfer,
		cType: CategoryTypeImmediate,
		name:  categoryNameTransfer,
	}

	CategoryTimer = Category{
		id:    CategoryIDTimer,
		cType: CategoryTypeScheduled,
		name:  categoryNameTimer,
	}

	CategoryReplication = Category{
		id:    CategoryIDReplication,
		cType: CategoryTypeImmediate,
		name:  categoryNameReplication,
	}

	CategoryVisibility = Category{
		id:    CategoryIDVisibility,
		cType: CategoryTypeImmediate,
		name:  categoryNameVisibility,
	}

	CategoryArchival = Category{
		id:    CategoryIDArchival,
		cType: CategoryTypeScheduled,
		name:  categoryNameArchival,
	}

	CategoryMemoryTimer = Category{
		id:    CategoryIDMemoryTimer,
		cType: CategoryTypeScheduled,
		name:  categoryNameMemoryTimer,
	}

	CategoryOutbound = Category{
		id:    CategoryIDOutbound,
		cType: CategoryTypeImmediate,
		name:  categoryNameOutbound,
	}
)

func NewCategory(id int, cType CategoryType, name string) Category {
	return Category{
		id:    id,
		cType: cType,
		name:  name,
	}
}

func (c Category) ID() int {
	return c.id
}

func (c Category) Name() string {
	return c.name
}

func (c Category) Type() CategoryType {
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
