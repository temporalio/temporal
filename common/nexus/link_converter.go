// This file is duplicated in sdk-go/temporalnexus/link_converter.go.
// Any changes here or there must be replicated. This is temporary until the
// temporal repo updates to the most recent SDK version.

package nexus

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
)

const (
	urlSchemeTemporalKey          = "temporal"
	urlPathNamespaceKey           = "namespace"
	urlPathWorkflowIDKey          = "workflowID"
	urlPathActivityIDKey          = "activityID"
	urlPathOperationIDKey         = "operationID"
	urlPathRunIDKey               = "runID"
	urlPathWorkflowEventTemplate  = "/namespaces/%s/workflows/%s/%s/history"
	urlPathNexusOperationTemplate = "/namespaces/%s/nexus-operations/%s/%s/details"
	urlPathActivityTemplate       = "/namespaces/%s/activities/%s/%s/details"

	linkWorkflowEventReferenceTypeKey = "referenceType"
	linkEventIDKey                    = "eventID"
	linkEventTypeKey                  = "eventType"
	linkRequestIDKey                  = "requestID"
	linkCallbackRequestIDKey          = "callback-request-id"
)

var (
	rePatternNamespace   = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathNamespaceKey)
	rePatternWorkflowID  = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathWorkflowIDKey)
	rePatternActivityID  = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathActivityIDKey)
	rePatternOperationID = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathOperationIDKey)
	rePatternRunID       = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathRunIDKey)
	urlPathRE            = regexp.MustCompile(fmt.Sprintf(
		`^/namespaces/%s/workflows/%s/%s/history$`,
		rePatternNamespace,
		rePatternWorkflowID,
		rePatternRunID,
	))
	urlPathActivityRE = regexp.MustCompile(fmt.Sprintf(
		`^/namespaces/%s/activities/%s/%s/details$`,
		rePatternNamespace,
		rePatternActivityID,
		rePatternRunID,
	))
	urlPathNexusOperationRE = regexp.MustCompile(fmt.Sprintf(
		`^/namespaces/%s/nexus-operations/%s/%s/details$`,
		rePatternNamespace,
		rePatternOperationID,
		rePatternRunID,
	))
	eventReferenceType     = string((&commonpb.Link_WorkflowEvent_EventReference{}).ProtoReflect().Descriptor().Name())
	requestIDReferenceType = string((&commonpb.Link_WorkflowEvent_RequestIdReference{}).ProtoReflect().Descriptor().Name())
)

// ConvertLinkNexusOperationToNexusLink converts a Link_NexusOperation type to Nexus Link.
func ConvertLinkNexusOperationToNexusLink(no *commonpb.Link_NexusOperation) nexus.Link {
	u := &url.URL{
		Scheme: urlSchemeTemporalKey,
		Path:   fmt.Sprintf(urlPathNexusOperationTemplate, no.GetNamespace(), no.GetOperationId(), no.GetRunId()),
		RawPath: fmt.Sprintf(
			urlPathNexusOperationTemplate,
			url.PathEscape(no.GetNamespace()),
			url.PathEscape(no.GetOperationId()),
			url.PathEscape(no.GetRunId()),
		),
	}

	return nexus.Link{
		URL:  u,
		Type: string(no.ProtoReflect().Descriptor().FullName()),
	}
}

// ConvertNexusLinkToLinkNexusOperation converts a Nexus Link to Link_NexusOperation.
func ConvertNexusLinkToLinkNexusOperation(l nexus.Link) (*commonpb.Link_NexusOperation, error) {
	expectedType := (&commonpb.Link_NexusOperation{}).ProtoReflect().Descriptor().FullName()
	if l.Type != string(expectedType) {
		return nil, fmt.Errorf("cannot parse link type %q to %q", l.Type, expectedType)
	}

	no, err := parseNexusOperationURL(l.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_NexusOperation: %w", err)
	}
	return no, nil
}

func parseNexusOperationURL(u *url.URL) (*commonpb.Link_NexusOperation, error) {
	if u == nil {
		return nil, errors.New("missing URL")
	}
	if u.Scheme != urlSchemeTemporalKey {
		return nil, fmt.Errorf("invalid scheme: %s", u.Scheme)
	}

	matches := urlPathNexusOperationRE.FindStringSubmatch(u.EscapedPath())
	if len(matches) != 4 {
		return nil, errors.New("malformed URL path")
	}

	no := &commonpb.Link_NexusOperation{}
	var err error
	no.Namespace, err = url.PathUnescape(matches[urlPathNexusOperationRE.SubexpIndex(urlPathNamespaceKey)])
	if err != nil {
		return nil, err
	}

	no.OperationId, err = url.PathUnescape(matches[urlPathNexusOperationRE.SubexpIndex(urlPathOperationIDKey)])
	if err != nil {
		return nil, err
	}

	no.RunId, err = url.PathUnescape(matches[urlPathNexusOperationRE.SubexpIndex(urlPathRunIDKey)])
	if err != nil {
		return nil, err
	}
	return no, nil
}

// ConvertLinkActivityToNexusLink converts a Link_Activity type to Nexus Link.
//
// NOTE: Experimental
func ConvertLinkActivityToNexusLink(a *commonpb.Link_Activity) nexus.Link {
	u := &url.URL{
		Scheme: urlSchemeTemporalKey,
		Path:   fmt.Sprintf(urlPathActivityTemplate, a.GetNamespace(), a.GetActivityId(), a.GetRunId()),
		RawPath: fmt.Sprintf(
			urlPathActivityTemplate,
			url.PathEscape(a.GetNamespace()),
			url.PathEscape(a.GetActivityId()),
			url.PathEscape(a.GetRunId()),
		),
	}

	return nexus.Link{
		URL:  u,
		Type: string(a.ProtoReflect().Descriptor().FullName()),
	}
}

// ConvertNexusLinkToLinkActivity converts a Nexus Link to Link_Activity.
//
// NOTE: Experimental
func ConvertNexusLinkToLinkActivity(link nexus.Link) (*commonpb.Link_Activity, error) {
	a := &commonpb.Link_Activity{}
	if link.Type != string(a.ProtoReflect().Descriptor().FullName()) {
		return nil, fmt.Errorf(
			"cannot parse link type %q to %q",
			link.Type,
			a.ProtoReflect().Descriptor().FullName(),
		)
	}

	if link.URL.Scheme != urlSchemeTemporalKey {
		return nil, fmt.Errorf(
			"failed to parse link to Link_Activity: invalid scheme: %s",
			link.URL.Scheme,
		)
	}

	matches := urlPathActivityRE.FindStringSubmatch(link.URL.EscapedPath())
	if len(matches) != 4 {
		return nil, errors.New("failed to parse link to Link_Activity: malformed URL path")
	}

	var err error
	a.Namespace, err = url.PathUnescape(matches[urlPathActivityRE.SubexpIndex(urlPathNamespaceKey)])
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_Activity: %w", err)
	}

	a.ActivityId, err = url.PathUnescape(matches[urlPathActivityRE.SubexpIndex(urlPathActivityIDKey)])
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_Activity: %w", err)
	}

	a.RunId, err = url.PathUnescape(matches[urlPathActivityRE.SubexpIndex(urlPathRunIDKey)])
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_Activity: %w", err)
	}
	return a, nil
}

// ConvertLinkActivityToNexusLink converts a Link_Callback type to Nexus Link.
func ConvertLinkCallbackToNexusLink(c *commonpb.Link_Callback) (nexus.Link, error) {
	// Worker callbacks are only supported for SANO operations.
	ty := c.GetExecution().GetType()
	if ty != enumspb.EXECUTION_TYPE_NEXUS_OPERATION {
		return nexus.Link{}, fmt.Errorf("unsupported execution for linking: %v", ty)
	}

	// Use the existing SANO URL format, and add the callback request ID as a
	// URL query parameter.
	apiLink := &commonpb.Link_NexusOperation{
		Namespace:   c.GetNamespace(),
		OperationId: c.GetExecution().GetBusinessId(),
		RunId:       c.GetExecution().GetRunId(),
	}
	nexusLink := ConvertLinkNexusOperationToNexusLink(apiLink)

	nexusLink.URL.RawQuery = "callback-request-id=" + url.QueryEscape(c.GetRequestId())
	nexusLink.Type = string(c.ProtoReflect().Descriptor().FullName())
	return nexusLink, nil
}

// ConvertNexusLinkToLinkCallback converts a Nexus Link to Link_Callback.
func ConvertNexusLinkToLinkCallback(link nexus.Link) (*commonpb.Link_Callback, error) {
	c := &commonpb.Link_Callback{}
	if link.Type != string(c.ProtoReflect().Descriptor().FullName()) {
		return nil, fmt.Errorf(
			"cannot parse link type %q to %q",
			link.Type,
			c.ProtoReflect().Descriptor().FullName(),
		)
	}

	// The URL format is shared with Link_NexusOperation, with the callback request
	// ID carried as a query parameter.
	no, err := parseNexusOperationURL(link.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_Callback: %w", err)
	}

	requestID := link.URL.Query().Get(linkCallbackRequestIDKey)
	if requestID == "" {
		return nil, fmt.Errorf(
			"failed to parse link to Link_Callback: missing %q query parameter",
			linkCallbackRequestIDKey,
		)
	}

	c.Namespace = no.GetNamespace()
	c.Execution = &commonpb.Execution{
		Type:       enumspb.EXECUTION_TYPE_NEXUS_OPERATION,
		BusinessId: no.GetOperationId(),
		RunId:      no.GetRunId(),
	}
	c.RequestId = requestID
	return c, nil
}

// ConvertLinkWorkflowEventToNexusLink converts a Link_WorkflowEvent type to Nexus Link.
//
// NOTE: Experimental
func ConvertLinkWorkflowEventToNexusLink(we *commonpb.Link_WorkflowEvent) nexus.Link {
	u := &url.URL{
		Scheme: urlSchemeTemporalKey,
		Path:   fmt.Sprintf(urlPathWorkflowEventTemplate, we.GetNamespace(), we.GetWorkflowId(), we.GetRunId()),
		RawPath: fmt.Sprintf(
			urlPathWorkflowEventTemplate,
			url.PathEscape(we.GetNamespace()),
			url.PathEscape(we.GetWorkflowId()),
			url.PathEscape(we.GetRunId()),
		),
	}

	switch ref := we.GetReference().(type) {
	case *commonpb.Link_WorkflowEvent_EventRef:
		u.RawQuery = convertLinkWorkflowEventEventReferenceToURLQuery(ref.EventRef)
	case *commonpb.Link_WorkflowEvent_RequestIdRef:
		u.RawQuery = convertLinkWorkflowEventRequestIdReferenceToURLQuery(ref.RequestIdRef)
	}
	return nexus.Link{
		URL:  u,
		Type: string(we.ProtoReflect().Descriptor().FullName()),
	}
}

// ConvertNexusLinkToLinkWorkflowEvent converts a Nexus Link to Link_WorkflowEvent.
//
// NOTE: Experimental
func ConvertNexusLinkToLinkWorkflowEvent(link nexus.Link) (*commonpb.Link_WorkflowEvent, error) {
	we := &commonpb.Link_WorkflowEvent{}
	if link.Type != string(we.ProtoReflect().Descriptor().FullName()) {
		return nil, fmt.Errorf(
			"cannot parse link type %q to %q",
			link.Type,
			we.ProtoReflect().Descriptor().FullName(),
		)
	}

	if link.URL.Scheme != urlSchemeTemporalKey {
		return nil, fmt.Errorf(
			"failed to parse link to Link_WorkflowEvent: invalid scheme: %s",
			link.URL.Scheme,
		)
	}

	matches := urlPathRE.FindStringSubmatch(link.URL.EscapedPath())
	if len(matches) != 4 {
		return nil, errors.New("failed to parse link to Link_WorkflowEvent: malformed URL path")
	}

	var err error
	we.Namespace, err = url.PathUnescape(matches[urlPathRE.SubexpIndex(urlPathNamespaceKey)])
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_WorkflowEvent: %w", err)
	}

	we.WorkflowId, err = url.PathUnescape(matches[urlPathRE.SubexpIndex(urlPathWorkflowIDKey)])
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_WorkflowEvent: %w", err)
	}

	we.RunId, err = url.PathUnescape(matches[urlPathRE.SubexpIndex(urlPathRunIDKey)])
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_WorkflowEvent: %w", err)
	}

	switch refType := link.URL.Query().Get(linkWorkflowEventReferenceTypeKey); refType {
	case eventReferenceType:
		eventRef, err := convertURLQueryToLinkWorkflowEventEventReference(link.URL.Query())
		if err != nil {
			return nil, fmt.Errorf("failed to parse link to Link_WorkflowEvent: %w", err)
		}
		we.Reference = &commonpb.Link_WorkflowEvent_EventRef{
			EventRef: eventRef,
		}
	case requestIDReferenceType:
		requestIDRef, err := convertURLQueryToLinkWorkflowEventRequestIdReference(link.URL.Query())
		if err != nil {
			return nil, fmt.Errorf("failed to parse link to Link_WorkflowEvent: %w", err)
		}
		we.Reference = &commonpb.Link_WorkflowEvent_RequestIdRef{
			RequestIdRef: requestIDRef,
		}
	default:
		return nil, fmt.Errorf(
			"failed to parse link to Link_WorkflowEvent: unknown reference type: %q",
			refType,
		)
	}

	return we, nil
}

func convertLinkWorkflowEventEventReferenceToURLQuery(eventRef *commonpb.Link_WorkflowEvent_EventReference) string {
	values := url.Values{}
	values.Set(linkWorkflowEventReferenceTypeKey, eventReferenceType)
	if eventRef.GetEventId() > 0 {
		values.Set(linkEventIDKey, strconv.FormatInt(eventRef.GetEventId(), 10))
	}
	values.Set(linkEventTypeKey, eventRef.GetEventType().String())
	return values.Encode()
}

func convertURLQueryToLinkWorkflowEventEventReference(queryValues url.Values) (*commonpb.Link_WorkflowEvent_EventReference, error) {
	var err error
	eventRef := &commonpb.Link_WorkflowEvent_EventReference{}
	eventIDValue := queryValues.Get(linkEventIDKey)
	if eventIDValue != "" {
		eventRef.EventId, err = strconv.ParseInt(queryValues.Get(linkEventIDKey), 10, 64)
		if err != nil {
			return nil, err
		}
	}
	eventRef.EventType, err = enumspb.EventTypeFromString(queryValues.Get(linkEventTypeKey))
	if err != nil {
		return nil, err
	}
	return eventRef, nil
}

func convertLinkWorkflowEventRequestIdReferenceToURLQuery(requestIDRef *commonpb.Link_WorkflowEvent_RequestIdReference) string {
	values := url.Values{}
	values.Set(linkWorkflowEventReferenceTypeKey, requestIDReferenceType)
	values.Set(linkRequestIDKey, requestIDRef.GetRequestId())
	values.Set(linkEventTypeKey, requestIDRef.GetEventType().String())
	return values.Encode()
}

func convertURLQueryToLinkWorkflowEventRequestIdReference(queryValues url.Values) (*commonpb.Link_WorkflowEvent_RequestIdReference, error) {
	var err error
	requestIDRef := &commonpb.Link_WorkflowEvent_RequestIdReference{
		RequestId: queryValues.Get(linkRequestIDKey),
	}
	requestIDRef.EventType, err = enumspb.EventTypeFromString(queryValues.Get(linkEventTypeKey))
	if err != nil {
		return nil, err
	}
	return requestIDRef, nil
}
