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
	}
}
