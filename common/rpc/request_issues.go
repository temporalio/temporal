package rpc

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequestIssues is a collection of request validation issues for an RPC. The zero value is valid to use.
type RequestIssues struct {
	issues []string
}

// Append an issue to the set.
func (ri *RequestIssues) Append(issue string) {
	ri.issues = append(ri.issues, issue)
}

// Appendf appends a formatted issue to the set.
func (ri *RequestIssues) Appendf(format string, args ...interface{}) {
	ri.Append(fmt.Sprintf(format, args...))
}

// GetError returns a gRPC INVALID_ARGUMENT error representing the issues in the set or nil if there are no issues.
func (ri *RequestIssues) GetError() error {
	if len(ri.issues) == 0 {
		return nil
	}
	return status.Error(codes.InvalidArgument, strings.Join(ri.issues, ", "))
}
