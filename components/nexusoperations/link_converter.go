// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexusoperations

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

func ConvertLinkWorkflowEventToNexusLink(we *commonpb.Link_WorkflowEvent) (nexus.Link, error) {
	u, err := url.Parse(fmt.Sprintf(
		"temporal:///namespaces/%s/workflows/%s/%s/history",
		url.PathEscape(we.GetNamespace()),
		url.PathEscape(we.GetWorkflowId()),
		url.PathEscape(we.GetRunId()),
	))
	if err != nil {
		return nexus.Link{}, err
	}

	linkType := ""
	switch ref := we.GetReference().(type) {
	case *commonpb.Link_WorkflowEvent_EventRef:
		u.RawQuery = convertLinkWorkflowEventEventReferenceToURLQuery(ref.EventRef)
		linkType = string(ref.EventRef.ProtoReflect().Descriptor().FullName())
	}
	return nexus.Link{URL: u, Type: linkType}, nil
}

func ConvertNexusLinkToLinkWorkflowEvent(link nexus.Link) (*commonpb.Link_WorkflowEvent, error) {
	we := &commonpb.Link_WorkflowEvent{}
	if !strings.HasPrefix(link.Type, string(we.ProtoReflect().Descriptor().FullName())+".") {
		return nil, fmt.Errorf(
			"cannot parse link type %q to %q",
			link.Type,
			we.ProtoReflect().Descriptor().FullName(),
		)
	}

	pathParts := strings.Split(link.URL.Path, "/")
	if len(pathParts) != 7 {
		return nil, fmt.Errorf("failed to parse link to Link_WorkflowEvent")
	}
	if pathParts[0] != "" || pathParts[1] != "namespaces" || pathParts[3] != "workflows" || pathParts[6] != "history" {
		return nil, fmt.Errorf("failed to parse link to Link_WorkflowEvent")
	}
	we.Namespace = pathParts[2]
	we.WorkflowId = pathParts[4]
	we.RunId = pathParts[5]
	switch link.Type {
	case string((&commonpb.Link_WorkflowEvent_EventReference{}).ProtoReflect().Descriptor().FullName()):
		eventRef, err := convertURLQueryToLinkWorkflowEventEventReference(link.URL.RawQuery)
		if err != nil {
			return nil, err
		}
		we.Reference = &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: eventRef,
		}
	}

	return we, nil
}

func convertLinkWorkflowEventEventReferenceToURLQuery(eventRef *commonpb.Link_WorkflowEvent_EventReference) string {
	values := url.Values{
		"event_id":   []string{strconv.FormatInt(eventRef.GetEventId(), 10)},
		"event_type": []string{eventRef.GetEventType().String()},
	}
	return values.Encode()
}

func convertURLQueryToLinkWorkflowEventEventReference(query string) (*commonpb.Link_WorkflowEvent_EventReference, error) {
	eventRef := &commonpb.Link_WorkflowEvent_EventReference{}
	values, err := url.ParseQuery(query)
	if err != nil {
		return nil, err
	}
	if len(values["event_id"]) != 1 {
		return nil, fmt.Errorf(
			"invalid query values for %s: %s",
			eventRef.ProtoReflect().Descriptor().FullName(),
			query,
		)
	}
	if len(values["event_type"]) != 1 {
		return nil, fmt.Errorf(
			"invalid query values for %s: %s",
			eventRef.ProtoReflect().Descriptor().FullName(),
			query,
		)
	}
	eventID, err := strconv.ParseInt(values["event_id"][0], 10, 64)
	if err != nil {
		return nil, err
	}
	eventType, err := enumspb.EventTypeFromString(values["event_type"][0])
	if err != nil {
		return nil, err
	}
	return &commonpb.Link_WorkflowEvent_EventReference{
		EventId:   eventID,
		EventType: eventType,
	}, nil
}
