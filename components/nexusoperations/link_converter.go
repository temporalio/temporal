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

const (
	linkWorkflowEventReferenceTypeKey = "referenceType"
	linkEventReferenceEventIDKey      = "eventID"
	linkEventReferenceEventTypeKey    = "eventType"
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

	switch ref := we.GetReference().(type) {
	case *commonpb.Link_WorkflowEvent_EventRef:
		u.RawQuery = convertLinkWorkflowEventEventReferenceToURLQuery(ref.EventRef)
	}
	return nexus.Link{
		URL:  u,
		Type: string(we.ProtoReflect().Descriptor().FullName()),
	}, nil
}

func ConvertNexusLinkToLinkWorkflowEvent(link nexus.Link) (*commonpb.Link_WorkflowEvent, error) {
	we := &commonpb.Link_WorkflowEvent{}
	if link.Type != string(we.ProtoReflect().Descriptor().FullName()) {
		return nil, fmt.Errorf(
			"cannot parse link type %q to %q",
			link.Type,
			we.ProtoReflect().Descriptor().FullName(),
		)
	}

	if link.URL.Scheme != "temporal" {
		return nil, fmt.Errorf("failed to parse link to Link_WorkflowEvent")
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
	switch link.URL.Query().Get(linkWorkflowEventReferenceTypeKey) {
	case string((&commonpb.Link_WorkflowEvent_EventReference{}).ProtoReflect().Descriptor().Name()):
		eventRef, err := convertURLQueryToLinkWorkflowEventEventReference(link.URL.Query())
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
		linkWorkflowEventReferenceTypeKey: []string{string(eventRef.ProtoReflect().Descriptor().Name())},
		linkEventReferenceEventIDKey:      []string{strconv.FormatInt(eventRef.GetEventId(), 10)},
		linkEventReferenceEventTypeKey:    []string{eventRef.GetEventType().String()},
	}
	return values.Encode()
}

func convertURLQueryToLinkWorkflowEventEventReference(queryValues url.Values) (*commonpb.Link_WorkflowEvent_EventReference, error) {
	eventID, err := strconv.ParseInt(queryValues.Get(linkEventReferenceEventIDKey), 10, 64)
	if err != nil {
		return nil, err
	}
	eventType, err := enumspb.EventTypeFromString(queryValues.Get(linkEventReferenceEventTypeKey))
	if err != nil {
		return nil, err
	}
	return &commonpb.Link_WorkflowEvent_EventReference{
		EventId:   eventID,
		EventType: eventType,
	}, nil
}
