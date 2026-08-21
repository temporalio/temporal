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
	constraintField struct {
		GoName   string
		JSONName string
		Value    string
	}
	settingPrecedence struct {
		Name       string
		GoArgs     string
		Precedence [][]constraintField
	}

	dynamicConfigData struct {
		Types       []settingType
		Precedences []settingPrecedence
	}
)

func (p settingPrecedence) Fields() []constraintField {
	var fields []constraintField
	seen := make(map[string]struct{})
	for _, constraints := range p.Precedence {
		for _, constraint := range constraints {
			if _, ok := seen[constraint.JSONName]; ok {
				continue
			}
			seen[constraint.JSONName] = struct{}{}
			fields = append(fields, constraint)
		}
	}
	return fields
}

var (
	//go:embed dynamic_config.tmpl
	dynamicConfigTemplate string
	namespaceConstraint   = constraintField{
		GoName:   "Namespace",
		JSONName: "namespace",
		Value:    "namespace",
	}
	namespaceIDConstraint = constraintField{
		GoName:   "NamespaceID",
		JSONName: "namespaceId",
		Value:    "namespaceID.String()",
	}
	taskQueueNameConstraint = constraintField{
		GoName:   "TaskQueueName",
		JSONName: "taskQueueName",
		Value:    "taskQueue",
	}
	taskQueueTypeConstraint = constraintField{
		GoName:   "TaskQueueType",
		JSONName: "taskQueueType",
		Value:    "taskQueueType",
	}
	shardIDConstraint = constraintField{
		GoName:   "ShardID",
		JSONName: "shardId",
		Value:    "shardID",
	}
	taskTypeConstraint = constraintField{
		GoName:   "TaskType",
		JSONName: "taskType",
		Value:    "taskType",
	}
	destinationConstraint = constraintField{
		GoName:   "Destination",
		JSONName: "destination",
		Value:    "destination",
	}
	chasmTaskTypeConstraint = constraintField{
		GoName:   "ChasmTaskType",
		JSONName: "chasmTaskType",
		Value:    "chasmTaskType",
	}

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
				Name:       "Global",
				GoArgs:     "",
				Precedence: [][]constraintField{{}},
			},
			{
				Name:       "Namespace",
				GoArgs:     "namespace string",
				Precedence: [][]constraintField{{namespaceConstraint}, {}},
			},
			{
				Name:       "NamespaceID",
				GoArgs:     "namespaceID namespace.ID",
				Precedence: [][]constraintField{{namespaceIDConstraint}, {}},
			},
			{
				Name:   "TaskQueue",
				GoArgs: "namespace string, taskQueue string, taskQueueType enumspb.TaskQueueType",
				// A task-queue-name-only filter applies to a single task queue name across all
				// namespaces, with higher precedence than a namespace-only filter. This is intended to
				// be used by the default partition count and is probably not useful otherwise.
				Precedence: [][]constraintField{
					{namespaceConstraint, taskQueueNameConstraint, taskQueueTypeConstraint},
					{namespaceConstraint, taskQueueNameConstraint},
					{taskQueueNameConstraint},
					{namespaceConstraint},
					{},
				},
			},
			{
				Name:       "ShardID",
				GoArgs:     "shardID int32",
				Precedence: [][]constraintField{{shardIDConstraint}, {}},
			},
			{
				Name:       "TaskType",
				GoArgs:     "taskType enumsspb.TaskType",
				Precedence: [][]constraintField{{taskTypeConstraint}, {}},
			},
			{
				Name:   "Destination",
				GoArgs: "namespace string, destination string",
				Precedence: [][]constraintField{
					{namespaceConstraint, destinationConstraint},
					{destinationConstraint},
					{namespaceConstraint},
					{},
				},
			},
			{
				Name:       "ChasmTaskType",
				GoArgs:     "chasmTaskType string",
				Precedence: [][]constraintField{{chasmTaskTypeConstraint}, {}},
			},
		}}
)

func main() {
	codegen.GenerateTemplateToFile(dynamicConfigTemplate, data, "", "setting")
}
