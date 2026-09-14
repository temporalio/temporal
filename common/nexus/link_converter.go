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
	urlPathRunIDKey               = "runID"
	urlPathWorkflowEventTemplate  = "/namespaces/%s/workflows/%s/%s/history"
	urlPathNexusOperationTemplate = "/namespaces/%s/nexus-operations/%s/%s/details"
	urlPathActivityTemplate       = "/namespaces/%s/activities/%s/%s/details"

	// A callback link addresses one callback attached to an execution, so its path names the
	// execution the way the links above do and then selects the callback by request ID:
	// /namespaces/{ns}/{executionType}/{businessID}/{runID}/callbacks/{requestID}
	urlPathCallbackTemplate     = "/namespaces/%s/%s/%s/%s/callbacks/%s"
	urlPathExecutionTypeKey     = "executionType"
	urlPathBusinessIDKey        = "businessID"
	urlPathCallbackRequestIDKey = "callbackRequestID"

	linkWorkflowEventReferenceTypeKey = "referenceType"
	linkEventIDKey                    = "eventID"
	linkEventTypeKey                  = "eventType"
	linkRequestIDKey                  = "requestID"
	// linkComponentPathKey carries Link_Callback.component_path, repeated once per segment, in order.
	linkComponentPathKey = "componentPath"
)

var (
	rePatternNamespace  = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathNamespaceKey)
	rePatternWorkflowID = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathWorkflowIDKey)
	rePatternActivityID = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathActivityIDKey)
	rePatternRunID      = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathRunIDKey)
	urlPathRE           = regexp.MustCompile(fmt.Sprintf(
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
	rePatternBusinessID        = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathBusinessIDKey)
	rePatternCallbackRequestID = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathCallbackRequestIDKey)
	rePatternExecutionType     = fmt.Sprintf(`(?P<%s>[^/]+)`, urlPathExecutionTypeKey)
	urlPathCallbackRE          = regexp.MustCompile(fmt.Sprintf(
		`^/namespaces/%s/%s/%s/%s/callbacks/%s$`,
		rePatternNamespace,
		rePatternExecutionType,
		rePatternBusinessID,
		rePatternRunID,
		rePatternCallbackRequestID,
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

// callbackLinkExecutionTypes maps the type of execution a callback is attached to onto the URL
// path segment naming it. The segment is spelled the way the other link URLs in this file already
// spell that kind of execution ("workflows", "activities", "nexus-operations"), so every
// temporal:// link addresses an execution the same way.
var callbackLinkExecutionTypes = map[enumspb.ExecutionType]string{
	enumspb.EXECUTION_TYPE_WORKFLOW:        "workflows",
	enumspb.EXECUTION_TYPE_ACTIVITY:        "activities",
	enumspb.EXECUTION_TYPE_NEXUS_OPERATION: "nexus-operations",
}

// callbackLinkExecutionType is the inverse of callbackLinkExecutionTypes: it maps a URL path
// segment back onto the execution type it names.
func callbackLinkExecutionType(segment string) (enumspb.ExecutionType, bool) {
	for executionType, candidate := range callbackLinkExecutionTypes {
		if candidate == segment {
			return executionType, true
		}
	}
	return enumspb.EXECUTION_TYPE_UNSPECIFIED, false
}

// ConvertLinkCallbackToNexusLink converts a Link_Callback type to a Nexus Link. It fails when the
// callback's execution type has no URL path segment.
func ConvertLinkCallbackToNexusLink(cb *commonpb.Link_Callback) (nexus.Link, error) {
	execution := cb.GetExecution()
	executionType, ok := callbackLinkExecutionTypes[execution.GetType()]
	if !ok {
		return nexus.Link{}, fmt.Errorf(
			"failed to convert Link_Callback to link: unsupported execution type: %s",
			execution.GetType(),
		)
	}

	u := &url.URL{
		Scheme: urlSchemeTemporalKey,
		Path: fmt.Sprintf(
			urlPathCallbackTemplate,
			cb.GetNamespace(),
			executionType,
			execution.GetBusinessId(),
			execution.GetRunId(),
			cb.GetRequestId(),
		),
		RawPath: fmt.Sprintf(
			urlPathCallbackTemplate,
			url.PathEscape(cb.GetNamespace()),
			executionType,
			url.PathEscape(execution.GetBusinessId()),
			url.PathEscape(execution.GetRunId()),
			url.PathEscape(cb.GetRequestId()),
		),
	}

	if componentPath := cb.GetComponentPath(); len(componentPath) > 0 {
		// Each segment is its own value of the same key. Encode sorts by key but keeps each key's
		// values in insertion order, so the segment order survives.
		values := url.Values{}
		for _, segment := range componentPath {
			values.Add(linkComponentPathKey, segment)
		}
		u.RawQuery = values.Encode()
	}

	return nexus.Link{
		URL:  u,
		Type: string(cb.ProtoReflect().Descriptor().FullName()),
	}, nil
}

// ConvertNexusLinkToLinkCallback converts a Nexus Link to Link_Callback variant.
func ConvertNexusLinkToLinkCallback(link nexus.Link) (*commonpb.Link_Callback, error) {
	cb := &commonpb.Link_Callback{}
	if link.Type != string(cb.ProtoReflect().Descriptor().FullName()) {
		return nil, fmt.Errorf(
			"cannot parse link type %q to %q",
			link.Type,
			cb.ProtoReflect().Descriptor().FullName(),
		)
	}

	if link.URL.Scheme != urlSchemeTemporalKey {
		return nil, fmt.Errorf(
			"failed to parse link to Link_Callback: invalid scheme: %s",
			link.URL.Scheme,
		)
	}

	matches := urlPathCallbackRE.FindStringSubmatch(link.URL.EscapedPath())
	if len(matches) != 6 {
		return nil, errors.New("failed to parse link to Link_Callback: malformed URL path")
	}

	segment := matches[urlPathCallbackRE.SubexpIndex(urlPathExecutionTypeKey)]
	executionType, ok := callbackLinkExecutionType(segment)
	if !ok {
		return nil, fmt.Errorf(
			"failed to parse link to Link_Callback: unsupported execution type: %q",
			segment,
		)
	}
	execution := &commonpb.Execution{Type: executionType}
	cb.Execution = execution

	var err error
	cb.Namespace, err = url.PathUnescape(matches[urlPathCallbackRE.SubexpIndex(urlPathNamespaceKey)])
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_Callback: %w", err)
	}

	execution.BusinessId, err = url.PathUnescape(matches[urlPathCallbackRE.SubexpIndex(urlPathBusinessIDKey)])
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_Callback: %w", err)
	}

	execution.RunId, err = url.PathUnescape(matches[urlPathCallbackRE.SubexpIndex(urlPathRunIDKey)])
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_Callback: %w", err)
	}

	cb.RequestId, err = url.PathUnescape(matches[urlPathCallbackRE.SubexpIndex(urlPathCallbackRequestIDKey)])
	if err != nil {
		return nil, fmt.Errorf("failed to parse link to Link_Callback: %w", err)
	}

	cb.ComponentPath = link.URL.Query()[linkComponentPathKey]
	return cb, nil
}
