package main

import (
	_ "embed"

	"go.temporal.io/server/cmd/tools/codegen"
)

type (
	settingType struct {
		Name      string
		GoType    string
		IsGeneric bool
	}
	settingPrecedence struct {
		Name   string
		GoArgs string
		Expr   string
	}

	dynamicConfigData struct {
		Types       []settingType
		Precedences []settingPrecedence
	}
)

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
				Name:   "Namespace",
				GoArgs: "namespace string",
				Expr:   "[]Constraints{{Namespace: namespace}, {}}",
			},
			{
				Name:   "NamespaceID",
				GoArgs: "namespaceID namespace.ID",
				Expr:   "[]Constraints{{NamespaceID: namespaceID.String()}, {}}",
			},
			{
				Name:   "TaskQueue",
				GoArgs: "namespace string, taskQueue string, taskQueueType enumspb.TaskQueueType",
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
				Name:   "ShardID",
				GoArgs: "shardID int32",
				Expr:   "[]Constraints{{ShardID: shardID}, {}}",
			},
			{
				Name:   "TaskType",
				GoArgs: "taskType enumsspb.TaskType",
				Expr:   "[]Constraints{{TaskType: taskType}, {}}",
			},
			{
				Name:   "Destination",
				GoArgs: "namespace string, destination string",
				Expr: `[]Constraints{
			{Namespace: namespace, Destination: destination},
			{Destination: destination},
			{Namespace: namespace},
			{},
		}`,
			},
		}}
)

func main() {
	codegen.GenerateTemplateToFile(dynamicConfigTemplate, data, "", "setting")
}
