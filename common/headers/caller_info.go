package headers

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const (
	CallerTypeOperator = "operator"
	// CallerTypeAPI is for user foreground requests.
	CallerTypeAPI = "api"
	// CallerTypeBackground is mostly for user background task. Some overrides may apply.
	CallerTypeBackground = "background"
	// CallerTypeReplication is for global namespace live traffic.
	CallerTypeReplication = "replication"
	// CallerTypePreemptable is for user related maintenance operations.
	CallerTypePreemptable = "preemptable"

	CallerNameSystem = "system"
)

var (
	ValidCallerTypes = map[string]struct{}{
		CallerTypeOperator:    {},
		CallerTypeAPI:         {},
		CallerTypeBackground:  {},
		CallerTypePreemptable: {},
	}
)

var (
	SystemBackgroundCallerInfo = CallerInfo{
		CallerName: CallerNameSystem,
		CallerType: CallerTypeBackground,
	}
	SystemPreemptableCallerInfo = CallerInfo{
		CallerName: CallerNameSystem,
		CallerType: CallerTypePreemptable,
	}
	SystemReplicationCallerInfo = CallerInfo{
		CallerName: CallerNameSystem,
		CallerType: CallerTypeReplication,
	}
)

type (
	CallerInfo struct {
		// CallerName is the name of the caller.
		// It can either user namespace name or
		// the predefined CallerNameSystem.
		CallerName string

		// CallerType indicates if the call originates from
		// user API calls or from system background operations.
		CallerType string

		// CallOrigin is the first API method name in the call chain.
		// Currently, its value is valid only when CallerType is CallerTypeAPI or CallerTypeOperator.
		CallOrigin string
	}
)

// NewCallerInfo creates a new CallerInfo
func NewCallerInfo(
	callerName string,
	callerType string,
	callOrigin string,
) CallerInfo {
	return CallerInfo{
		CallerName: callerName,
		CallerType: callerType,
		CallOrigin: callOrigin,
	}
}

// NewBackgroundCallerInfo creates a new CallerInfo with Background callerType
// and empty callOrigin.
// This is equivalent to NewCallerInfo(callerName, CallerTypeBackground, "")
func NewBackgroundCallerInfo(
	callerName string,
) CallerInfo {
	return CallerInfo{
		CallerName: callerName,
		CallerType: CallerTypeBackground,
	}
}

// NewPreemptableCallerInfo creates a new CallerInfo with Preemptable callerType
// and empty callOrigin.
// This is equivalent to NewCallerInfo(callerName, CallerTypePreemptable, "")
func NewPreemptableCallerInfo(
	callerName string,
) CallerInfo {
	return CallerInfo{
		CallerName: callerName,
		CallerType: CallerTypePreemptable,
	}
}

// SetCallerInfo sets callerName, callerType and CallOrigin in the context.
// Existing values will be overwritten if new value is not empty.
// TODO: consider only set the caller info to golang context instead of grpc metadata
// and propagate to grpc outgoing context upon making an rpc call
func SetCallerInfo(
	ctx context.Context,
	info CallerInfo,
) context.Context {
	return setIncomingMD(ctx, map[string]string{
		CallerNameHeaderName: info.CallerName,
		CallerTypeHeaderName: info.CallerType,
		CallOriginHeaderName: info.CallOrigin,
	})
}

// SetCallerName set caller name in the context.
// Existing caller name will be overwritten if exists and new caller name is not empty.
func SetCallerName(
	ctx context.Context,
	callerName string,
) context.Context {
	return setIncomingMD(ctx, map[string]string{CallerNameHeaderName: callerName})
}

// SetCallerType set caller type in the context.
// Existing caller type will be overwritten if exists and new caller type is not empty.
func SetCallerType(
	ctx context.Context,
	callerType string,
) context.Context {
	return setIncomingMD(ctx, map[string]string{CallerTypeHeaderName: callerType})
}

// SetOrigin set call origin in the context.
// Existing call origin will be overwritten if exists and new call origin is not empty.
func SetOrigin(
	ctx context.Context,
	callOrigin string,
) context.Context {
	return setIncomingMD(ctx, map[string]string{CallOriginHeaderName: callOrigin})
}

func setIncomingMD(
	ctx context.Context,
	kv map[string]string,
) context.Context {
	mdIncoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		mdIncoming = metadata.MD{}
	}

	for k, v := range kv {
		if v != "" {
			mdIncoming.Set(k, v)
		}
	}

	return metadata.NewIncomingContext(ctx, mdIncoming)
}

// GetCallerInfo retrieves caller information from the context if exists. Empty value is returned
// if any piece of caller information is not specified in the context.
func GetCallerInfo(
	ctx context.Context,
) CallerInfo {
	values := GetValues(ctx, CallerNameHeaderName, CallerTypeHeaderName, CallOriginHeaderName)
	return CallerInfo{
		CallerName: values[0],
		CallerType: values[1],
		CallOrigin: values[2],
	}
}
