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

package utf8validator

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	// A Validator validates utf-8 in string fields of proto messages. The proto library
	// normally does this automatically, but we disable it for now as part of migration from
	// gogoproto, which did not validate. Eventually we'll remove this and enable it normally.
	Validator struct {
		logger  log.Logger
		metrics metrics.Handler

		checkBySource [sourceCount]dynamicconfig.FloatPropertyFn
		failBySource  [sourceCount]dynamicconfig.BoolPropertyFn
	}

	MessageSource int

	validation struct {
		badPath string
	}
)

const (
	SourceRPCRequest MessageSource = iota
	SourceRPCResponse
	SourcePersistence
	sourceCount
)

var (
	ErrInvalidUTF8          = errors.New("invalid utf-8 in string field")
	ErrInvalidMessageSource = errors.New("invalid message source")

	sourceNames = [sourceCount]string{
		"rpc-request",
		"rpc-response",
		"persistence",
	}

	// globalValidator holds one instance of Validator for use where we want to validate but
	// have no context to hold a reference to a Validator. If multiple services are running in
	// the same process it will be the last one constructed, but it doesn't matter much since
	// they should all behave the same (just with different log/metrics tags).
	globalValidator atomic.Value
)

func newValidator(
	logger log.Logger,
	metrics metrics.Handler,
	checkRPCRequest dynamicconfig.FloatPropertyFn,
	checkRPCResponse dynamicconfig.FloatPropertyFn,
	checkPersistence dynamicconfig.FloatPropertyFn,
	failRPCRequest dynamicconfig.BoolPropertyFn,
	failRPCResponse dynamicconfig.BoolPropertyFn,
	failPersistence dynamicconfig.BoolPropertyFn,
) *Validator {
	v := &Validator{
		logger:  logger,
		metrics: metrics,
		checkBySource: [sourceCount]dynamicconfig.FloatPropertyFn{
			checkRPCRequest, checkRPCResponse, checkPersistence,
		},
		failBySource: [sourceCount]dynamicconfig.BoolPropertyFn{
			failRPCRequest, failRPCResponse, failPersistence,
		},
	}
	globalValidator.Store(v)
	return v
}

// Validate maybe validates one proto message, depending on dynamic config
// and random sampling. If validation fails, this may still return no error, depending on
// dynamic config. This deliberately does not return a serviceerror because the decision of
// which serviceerror to use is contextual. The caller should wrap it in the appropriate
// serviceerror.
func Validate(
	m proto.Message,
	source MessageSource,
	tags ...tag.Tag,
) error {
	if v, ok := globalValidator.Load().(*Validator); ok {
		return v.Validate(m, source, tags...)
	}
	return nil
}

// Validate maybe validates one proto message, depending on dynamic config and random sampling.
// If validation fails, this may still return no error, depending on dynamic config. This
// deliberately does not return a serviceerror because the decision of which serviceerror to
// use is contextual. The caller should wrap it in the appropriate serviceerror.
func (v *Validator) Validate(
	m proto.Message,
	source MessageSource,
	tags ...tag.Tag,
) error {
	if source < 0 || source >= sourceCount {
		return ErrInvalidMessageSource
	}
	if rand.Float64() >= v.checkBySource[source]() {
		return nil
	}

	var validation validation
	ref := m.ProtoReflect()
	if validation.validateMessage(ref) {
		return nil
	}

	badPath := string(ref.Descriptor().Name()) + "." + validation.badPath

	v.logger.Warn("invalid utf-8 in string field", append([]tag.Tag{
		tag.NewStringTag("message-source", sourceNames[source]),
		tag.NewStringTag("message-path", badPath),
	}, tags...)...)
	metrics.UTF8ValidationErrors.With(v.metrics).Record(1)

	if v.failBySource[source]() {
		return ErrInvalidUTF8
	}
	return nil
}

var _ grpc.UnaryServerInterceptor = (*Validator)(nil).Intercept

// Intercept acts as grpc interceptor.
func (v *Validator) Intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	nsTags := make([]tag.Tag, 0, 1)
	if getNs, ok := req.(interface{ GetNamespace() string }); ok {
		nsTags = append(nsTags, tag.WorkflowNamespace(getNs.GetNamespace()))
	}

	if reqM, ok := req.(proto.Message); ok {
		err := v.Validate(reqM, SourceRPCRequest, nsTags...)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(err.Error())
		}
	}

	res, err := handler(ctx, req)

	if resM, ok := res.(proto.Message); ok {
		vErr := v.Validate(resM, SourceRPCResponse, nsTags...)
		if vErr != nil && err == nil {
			err = serviceerror.NewInternal(vErr.Error())
		}
	}
	return res, err
}

// validateMessage validates that no string fields in m have invalid utf-8. Returns true on
// valid, false on invalid. v.badPath is set to the path to the first field with invalid data.
func (v *validation) validateMessage(m protoreflect.Message) bool {
	m.Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
		return v.validateValue(fd, val, false)
	})
	return v.badPath == ""
}

// validateValue validates a single value as a field of an enclosing message. Returns true on
// valid, false on invalid. If returns false, v.badPath will be set. insideList is used because
// we have to use the same FieldDescriptor for the list and each individual item of the list.
func (v *validation) validateValue(fd protoreflect.FieldDescriptor, val protoreflect.Value, insideList bool) bool {
	if fd.IsList() && !insideList {
		for list, i := val.List(), 0; i < list.Len(); i++ {
			if !v.validateValue(fd, list.Get(i), true) {
				if before, after, found := strings.Cut(v.badPath, "."); found {
					v.badPath = fmt.Sprintf("%s.[%d].%s", before, i, after)
				} else {
					v.badPath = fmt.Sprintf("%s.[%d]", before, i)
				}
				return false
			}
		}
	} else if fd.IsMap() {
		val.Map().Range(func(mapKey protoreflect.MapKey, mapVal protoreflect.Value) bool {
			if !v.validateValue(fd.MapKey(), mapKey.Value(), false) ||
				!v.validateValue(fd.MapValue(), mapVal, false) {
				v.badPath = fmt.Sprintf("%s.[%q].%s", fd.Name(), mapKey.String(), v.badPath)
				return false
			}
			return true
		})
		return v.badPath == ""
	} else if fd.Kind() == protoreflect.MessageKind {
		if !v.validateMessage(val.Message()) {
			v.badPath = string(fd.Name()) + "." + v.badPath
			return false
		}
	} else if fd.Kind() == protoreflect.StringKind {
		if !utf8.ValidString(val.String()) {
			v.badPath = string(fd.Name())
			return false
		}
	}
	return true
}
