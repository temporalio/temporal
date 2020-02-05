// Copyright (c) 2017 Uber Technologies, Inc.
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
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/urfave/cli"

	"github.com/uber/cadence/.gen/go/admin"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

// AdminAddSearchAttribute to whitelist search attribute
func AdminAddSearchAttribute(c *cli.Context) {
	key := getRequiredOption(c, FlagSearchAttributesKey)
	valType := getRequiredIntOption(c, FlagSearchAttributesType)
	if !isValueTypeValid(valType) {
		ErrorAndExit("Unknown Search Attributes value type.", nil)
	}

	// ask user for confirmation
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Are you trying to add key [%s] with Type [%s]? Y/N\n",
		color.YellowString(key), color.YellowString(intValTypeToString(valType)))
	text, _ := reader.ReadString('\n')
	textLower := strings.ToLower(strings.TrimRight(text, "\n"))
	if textLower != "y" && textLower != "yes" {
		return
	}

	adminClient := cFactory.ServerAdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()
	request := &admin.AddSearchAttributeRequest{
		SearchAttribute: map[string]shared.IndexedValueType{
			key: shared.IndexedValueType(valType),
		},
		SecurityToken: common.StringPtr(c.String(FlagSecurityToken)),
	}

	err := adminClient.AddSearchAttribute(ctx, request)
	if err != nil {
		ErrorAndExit("Add search attribute failed.", err)
	}
	fmt.Println("Success")
}

// AdminDescribeCluster is used to dump information about the cluster
func AdminDescribeCluster(c *cli.Context) {
	adminClient := cFactory.ServerAdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.DescribeCluster(ctx)
	if err != nil {
		ErrorAndExit("Operation DescribeCluster failed.", err)
	}

	prettyPrintJSONObject(response)
}

func intValTypeToString(valType int) string {
	switch valType {
	case 0:
		return "String"
	case 1:
		return "Keyword"
	case 2:
		return "Int"
	case 3:
		return "Double"
	case 4:
		return "Bool"
	case 5:
		return "Datetime"
	default:
		return ""
	}
}

func isValueTypeValid(valType int) bool {
	return valType >= 0 && valType <= 5
}
