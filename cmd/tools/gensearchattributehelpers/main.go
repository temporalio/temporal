package main

import (
	_ "embed"
	"flag"
	"reflect"
	"regexp"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/cmd/tools/codegen"
)

type (
	eventData struct {
		AttributesTypeName string
	}

	searchAttributesHelpersData struct {
		Events []eventData
	}
)

var (
	//go:embed search_attribute_helpers.tmpl
	searchAttributeHelpersTemplate string

	// Is used to find attribute getters and extract the event type (match[1]).
	attributesGetterRegex = regexp.MustCompile("^Get(.+EventAttributes)$")
)

func main() {
	outPathFlag := flag.String("out", ".", "path to write generated files")
	flag.Parse()

	codegen.GenerateTemplateToFile(searchAttributeHelpersTemplate, getSearchAttributesHelpersData(), *outPathFlag, "event")
}

func getSearchAttributesHelpersData() searchAttributesHelpersData {
	sahd := searchAttributesHelpersData{}

	historyEventT := reflect.TypeOf((*historypb.HistoryEvent)(nil))

	for i := 0; i < historyEventT.NumMethod(); i++ {
		attributesGetter := historyEventT.Method(i)
		matches := attributesGetterRegex.FindStringSubmatch(attributesGetter.Name)
		if len(matches) < 2 {
			continue
		}
		if attributesGetter.Type.NumOut() != 1 {
			continue
		}
		if _, found := attributesGetter.Type.Out(0).MethodByName("GetSearchAttributes"); !found {
			continue
		}

		ed := eventData{
			AttributesTypeName: matches[1],
		}
		sahd.Events = append(sahd.Events, ed)
	}
	return sahd
}
