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
		// MapExpr builds the same precedence list from a ConstraintsMap instead of from
		// positional arguments, for the GetC accessors. Defined as methods in
		// common/dynamicconfig/constraints_map.go.
		MapExpr string
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
				Name:    "Global",
				MapExpr: "cm.globalPrecedence()",
				GoArgs:  "",
				Expr:    "[]Constraints{{}}",
			},
			{
				Name:    "Namespace",
				MapExpr: "cm.namespacePrecedence()",
				GoArgs:  "namespace string",
				Expr:    "[]Constraints{{Namespace: namespace}, {}}",
			},
			{
				Name:    "NamespaceID",
				MapExpr: "cm.namespaceIDPrecedence()",
				GoArgs:  "namespaceID namespace.ID",
				Expr:    "[]Constraints{{NamespaceID: namespaceID.String()}, {}}",
			},
			{
				Name:    "TaskQueue",
				MapExpr: "cm.taskQueuePrecedence()",
				GoArgs:  "namespace string, taskQueue string, taskQueueType enumspb.TaskQueueType",
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
				Name:    "ShardID",
				MapExpr: "cm.shardIDPrecedence()",
				GoArgs:  "shardID int32",
				Expr:    "[]Constraints{{ShardID: shardID}, {}}",
			},
			{
				Name:    "TaskType",
				MapExpr: "cm.taskTypePrecedence()",
				GoArgs:  "taskType enumsspb.TaskType",
				Expr:    "[]Constraints{{TaskType: taskType}, {}}",
			},
			{
				Name:    "Destination",
				MapExpr: "cm.destinationPrecedence()",
				GoArgs:  "namespace string, destination string",
				Expr: `[]Constraints{
			{Namespace: namespace, Destination: destination},
			{Destination: destination},
			{Namespace: namespace},
			{},
		}`,
			},
			{
				Name:    "ChasmTaskType",
				MapExpr: "cm.chasmTaskTypePrecedence()",
				GoArgs:  "chasmTaskType string",
				Expr:    "[]Constraints{{ChasmTaskType: chasmTaskType}, {}}",
			},
		}}
)

func main() {
	codegen.GenerateTemplateToFile(dynamicConfigTemplate, data, "", "setting")
}
