package api

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/tasks"
)

func GetTaskCategory(categoryID int, registry tasks.TaskCategoryRegistry) (tasks.Category, error) {
	category, ok := registry.GetCategoryByID(categoryID)
	if !ok {
		return tasks.Category{}, serviceerror.NewInvalidArgument(
			fmt.Sprintf("Invalid task category id: %v", categoryID),
		)
	}
	return category, nil
}
