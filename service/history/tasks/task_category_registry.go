package tasks

import (
	"fmt"
	"maps"
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
			CategoryOutbound.ID():    CategoryOutbound,
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
