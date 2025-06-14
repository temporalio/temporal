package model

import (
	"fmt"
	"reflect"
	"strings"

	"go.temporal.io/server/common/testing/stamp"
	"google.golang.org/grpc/metadata"
)

type IncomingAction[T any] struct {
	ActionID       stamp.ActID `validate:"required"`
	Cluster        stamp.ID    `validate:"required"`
	RequestID      string
	RequestHeaders metadata.MD
	Method         string
	Request        T

	// mutable (all fields need to be pointers)
	ValidationErrs *[]string
}

func (a IncomingAction[T]) String() string {
	var sb strings.Builder
	sb.WriteString("IncomingAction[\n")
	sb.WriteString(fmt.Sprintf("  Cluster: %s\n", a.Cluster))
	sb.WriteString(fmt.Sprintf("  Request: %T\n", a.Request))
	sb.WriteString(fmt.Sprintf("  RequestID: %s\n", a.RequestID))
	sb.WriteString(fmt.Sprintf("  Method: %s\n", a.Method))
	sb.WriteString("]")
	return sb.String()
}

func (a IncomingAction[T]) GetValidationErrors() []string {
	if a.ValidationErrs == nil {
		return nil
	}
	return *a.ValidationErrs
}

func (a IncomingAction[T]) AddValidationError(err string) {
	*a.ValidationErrs = append(*a.ValidationErrs, err)
}

func (a IncomingAction[T]) ID() stamp.ActID {
	return a.ActionID
}

func (a IncomingAction[T]) Route() string {
	var res string
	t := reflect.TypeOf(a.Request)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
		res = "*"
	}
	res += t.PkgPath() + "." + t.Name()
	return res
}

type OutgoingAction[T any] struct {
	ActID       stamp.ActID
	Response    T
	ResponseErr error
}

func (a OutgoingAction[T]) ID() stamp.ActID {
	return a.ActID
}

func (a OutgoingAction[T]) String() string {
	var sb strings.Builder
	sb.WriteString("OutgoingAction[\n")
	if a.ResponseErr != nil {
		sb.WriteString(fmt.Sprintf("  ResponseErr: %T\n", a.ResponseErr))
	} else {
		sb.WriteString(fmt.Sprintf("  Response: %T\n", a.Response))
	}
	sb.WriteString("]")
	return sb.String()
}
