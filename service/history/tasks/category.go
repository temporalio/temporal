package tasks

import (
	"fmt"
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

func (c Category) MarshalText() (text []byte, err error) {
	return []byte(fmt.Sprintf("%s(id:%d, type:%s)", c.name, c.id, c.cType)), nil
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
