package main

import (
	_ "embed"
	"strings"

	"go.temporal.io/server/cmd/tools/codegen"
)

type (
	settingType struct {
		Name      string
		GoType    string
		IsGeneric bool
	}
	settingPrecedence struct {
		Name           string
		GoArgs         string
		PropertyFnArgs []string
		Expr           string
	}

	dynamicConfigData struct {
		Types       []settingType
		Precedences []settingPrecedence
	}
)

func (p settingPrecedence) ConstraintDescription() string {
	lines := strings.Split(p.Expr, "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(lines[i])
	}
	description := strings.Join(lines, " ")
	description = strings.Replace(description, "[]Constraints{ ", "[]Constraints{", 1)
	if before, ok := strings.CutSuffix(description, ", }"); ok {
		description = before + "}"
	}
	return description
}

var (
	//go:embed dynamic_config.tmpl
	dynamicConfigTemplate string

	data = dynamicConfigData{
		Types: []settingType{
			{
				Name:   "Bool",
				GoType: "bool",
			},
			{
				Name:   "Int",
				GoType: "int",
			},
			{
				Name:   "Float",
				GoType: "float64",
			},
			{
				Name:   "String",
				GoType: "string",
			},
			{
				Name:   "Duration",
				GoType: "time.Duration",
			},
			{
				Name:   "Map",
				GoType: "map[string]any",
			},
			{
				Name:      "Typed",
				GoType:    "<generic>",
				IsGeneric: true, // this one is treated differently
			},
		},
		Precedences: []settingPrecedence{
			{
				Name:   "Global",
				GoArgs: "",
				Expr:   "[]Constraints{{}}",
			},
			{
				Name:           "Namespace",
				GoArgs:         "namespace string",
				PropertyFnArgs: []string{"constraints.Namespace"},
				Expr:           "[]Constraints{{Namespace: namespace}, {}}",
			},
			{
				Name:           "NamespaceID",
				GoArgs:         "namespaceID namespace.ID",
				PropertyFnArgs: []string{"namespace.ID(constraints.NamespaceID)"},
				Expr:           "[]Constraints{{NamespaceID: namespaceID.String()}, {}}",
			},
			{
				Name:   "TaskQueue",
				GoArgs: "namespace string, taskQueue string, taskQueueType enumspb.TaskQueueType",
				PropertyFnArgs: []string{
					"constraints.Namespace",
					"constraints.TaskQueueName",
					"constraints.TaskQueueType",
				},
				// A task-queue-name-only filter applies to a single task queue name across all
				// namespaces, with higher precedence than a namespace-only filter. This is intended to
				// be used by the default partition count and is probably not useful otherwise.
				Expr: `[]Constraints{
			{Namespace: namespace, TaskQueueName: taskQueue, TaskQueueType: taskQueueType},
			{Namespace: namespace, TaskQueueName: taskQueue},
			{TaskQueueName: taskQueue},
			{Namespace: namespace},
			{},
		}`,
			},
			{
				Name:           "ShardID",
				GoArgs:         "shardID int32",
				PropertyFnArgs: []string{"constraints.ShardID"},
				Expr:           "[]Constraints{{ShardID: shardID}, {}}",
			},
			{
				Name:           "TaskType",
				GoArgs:         "taskType enumsspb.TaskType",
				PropertyFnArgs: []string{"constraints.TaskType"},
				Expr:           "[]Constraints{{TaskType: taskType}, {}}",
			},
			{
				Name:           "Destination",
				GoArgs:         "namespace string, destination string",
				PropertyFnArgs: []string{"constraints.Namespace", "constraints.Destination"},
				Expr: `[]Constraints{
			{Namespace: namespace, Destination: destination},
			{Destination: destination},
			{Namespace: namespace},
			{},
		}`,
			},
			{
				Name:           "ChasmTaskType",
				GoArgs:         "chasmTaskType string",
				PropertyFnArgs: []string{"constraints.ChasmTaskType"},
				Expr:           "[]Constraints{{ChasmTaskType: chasmTaskType}, {}}",
			},
		}}
)

func main() {
	codegen.GenerateTemplateToFile(dynamicConfigTemplate, data, "", "setting")
}
