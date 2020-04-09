package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/urfave/cli"
	commonpb "go.temporal.io/temporal-proto/common"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
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

	adminClient := cFactory.AdminClient(c)
	ctx, cancel := newContext(c)
	defer cancel()
	request := &adminservice.AddSearchAttributeRequest{
		SearchAttribute: map[string]commonpb.IndexedValueType{
			key: commonpb.IndexedValueType(valType),
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
