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

package cli

import (
	"fmt"
	"strings"

	"github.com/urfave/cli"
)

// by default we don't require any namespace data. But this can be overridden by calling SetRequiredNamespaceDataKeys()
var requiredNamespaceDataKeys = []string{}

// SetRequiredNamespaceDataKeys will set requiredNamespaceDataKeys
func SetRequiredNamespaceDataKeys(keys []string) {
	requiredNamespaceDataKeys = keys
}

func checkRequiredNamespaceDataKVs(namespaceData map[string]string) error {
	//check requiredNamespaceDataKeys
	for _, k := range requiredNamespaceDataKeys {
		_, ok := namespaceData[k]
		if !ok {
			return fmt.Errorf("namespace data error, missing required key %v . All required keys: %v", k, requiredNamespaceDataKeys)
		}
	}
	return nil
}

func parseNamespaceDataKVs(namespaceDataStr string) (map[string]string, error) {
	kvstrs := strings.Split(namespaceDataStr, ",")
	kvMap := map[string]string{}
	for _, kvstr := range kvstrs {
		kv := strings.Split(kvstr, ":")
		if len(kv) != 2 {
			return kvMap, fmt.Errorf("namespace data format error. It must be k1:v2,k2:v2,...,kn:vn")
		}
		k := strings.TrimSpace(kv[0])
		v := strings.TrimSpace(kv[1])
		kvMap[k] = v
	}

	return kvMap, nil
}

func newNamespaceCommands() []cli.Command {
	return []cli.Command{
		{
			Name:    "register",
			Aliases: []string{"re"},
			Usage:   "Register workflow namespace",
			Flags:   registerNamespaceFlags,
			Action: func(c *cli.Context) {
				newNamespaceCLI(c, false).RegisterNamespace(c)
			},
		},
		{
			Name:    "update",
			Aliases: []string{"up", "u"},
			Usage:   "Update existing workflow namespace",
			Flags:   updateNamespaceFlags,
			Action: func(c *cli.Context) {
				newNamespaceCLI(c, false).UpdateNamespace(c)
			},
		},
		{
			Name:    "describe",
			Aliases: []string{"desc"},
			Usage:   "Describe existing workflow namespace",
			Flags:   describeNamespaceFlags,
			Action: func(c *cli.Context) {
				newNamespaceCLI(c, false).DescribeNamespace(c)
			},
		},
		{
			Name:    "list",
			Aliases: []string{"l"},
			Usage:   "List all namespaces",
			Flags:   listNamespacesFlags,
			Action: func(c *cli.Context) {
				newNamespaceCLI(c, false).ListNamespaces(c)
			},
		},
	}
}
