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

	"github.com/fatih/color"
	"github.com/urfave/cli"
	enumspb "go.temporal.io/temporal-proto/enums/v1"

	"github.com/temporalio/temporal/api/adminservice/v1"
)

// AdminAddSearchAttribute to whitelist search attribute
func AdminAddSearchAttribute(c *cli.Context) {
	key := getRequiredOption(c, FlagSearchAttributesKey)
	valType := getRequiredIntOption(c, FlagSearchAttributesType)
	if !isValueTypeValid(valType) {
		ErrorAndExit("Unknown Search Attributes value type.", nil)
	}

	// ask user for confirmation
	promptMsg := fmt.Sprintf("Are you trying to add key [%s] with Type [%s]? Y/N", color.YellowString(key), color.YellowString(intValTypeToString(valType)))
	prompt(promptMsg, c.GlobalBool(FlagAutoConfirm))

	adminClient := cFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()
	request := &adminservice.AddSearchAttributeRequest{
		SearchAttribute: map[string]enumspb.IndexedValueType{
			key: enumspb.IndexedValueType(valType),
		},
		SecurityToken: c.String(FlagSecurityToken),
	}

	_, err := adminClient.AddSearchAttribute(ctx, request)
	if err != nil {
		ErrorAndExit("Add search attribute failed.", err)
	}
	fmt.Println("Success")
}

// AdminDescribeCluster is used to dump information about the cluster
func AdminDescribeCluster(c *cli.Context) {
	adminClient := cFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()
	response, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
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
