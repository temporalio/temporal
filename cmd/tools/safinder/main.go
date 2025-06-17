package main

import (
	"fmt"
	"sort"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	_ "go.temporal.io/server/api/adminservice/v1" // for descriptor registration
)

const targetType = "temporal.api.common.v1.SearchAttributes"

type TreeNode struct {
	Name     string
	Children map[string]*TreeNode
	IsTarget bool
}

func main() {
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		for i := 0; i < file.Services().Len(); i++ {
			svc := file.Services().Get(i)
			if string(svc.FullName()) == "temporal.server.api.adminservice.v1.AdminService" {
				for j := 0; j < svc.Methods().Len(); j++ {
					method := svc.Methods().Get(j)

					inputRoot := &TreeNode{Name: "Input", Children: map[string]*TreeNode{}}
					outputRoot := &TreeNode{Name: "Output", Children: map[string]*TreeNode{}}

					foundInput := buildPrunedTree(method.Input(), inputRoot, map[protoreflect.FullName]bool{})
					foundOutput := buildPrunedTree(method.Output(), outputRoot, map[protoreflect.FullName]bool{})

					if !foundInput && !foundOutput {
						continue // skip this RPC entirely
					}

					fmt.Printf("🔹 RPC: %s\n", method.Name())
					if foundInput {
						fmt.Printf("  ✅ Input: %s\n", method.Input().FullName())
						printTree(inputRoot, "    ")
					}
					if foundOutput {
						fmt.Printf("  ✅ Output: %s\n", method.Output().FullName())
						printTree(outputRoot, "    ")
					}
					fmt.Println()
				}
			}
		}
		return true
	})
}

// buildPrunedTree returns true if the subtree contains or leads to SearchAttributes
func buildPrunedTree(md protoreflect.MessageDescriptor, node *TreeNode, visited map[protoreflect.FullName]bool) bool {
	if visited[md.FullName()] {
		return false
	}
	visited[md.FullName()] = true

	found := false
	for i := 0; i < md.Fields().Len(); i++ {
		field := md.Fields().Get(i)
		if field.Kind() != protoreflect.MessageKind {
			continue
		}
		msg := field.Message()
		fieldName := string(field.Name())

		child := &TreeNode{Name: fieldName, Children: map[string]*TreeNode{}}

		if string(msg.FullName()) == targetType {
			child.IsTarget = true
			node.Children[fieldName] = child
			found = true
		} else if buildPrunedTree(msg, child, visited) {
			node.Children[fieldName] = child
			found = true
		}
	}

	delete(visited, md.FullName()) // allow re-visiting other paths
	return found
}

func printTree(node *TreeNode, prefix string) {
	// Sort children for consistent output
	keys := make([]string, 0, len(node.Children))
	for k := range node.Children {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i, k := range keys {
		child := node.Children[k]
		last := i == len(keys)-1
		connector := "├── "
		childPrefix := "│   "
		if last {
			connector = "└── "
			childPrefix = "    "
		}
		label := child.Name
		if child.IsTarget {
			label += " (SearchAttributes)"
		}
		fmt.Printf("%s%s%s\n", prefix, connector, label)
		printTree(child, prefix+childPrefix)
	}
}
