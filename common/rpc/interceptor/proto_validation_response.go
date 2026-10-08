package interceptor

import (
	"errors"
	"reflect"
	"regexp"

	"buf.build/go/protovalidate"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/softassert"
	"google.golang.org/protobuf/proto"
)

const maxProtoValidationDiagnostics = 10

var protoValidationCollectionIndex = regexp.MustCompile(`\[(?:"(?:\\.|[^"\\])*"|[^\]]*)\]`)

type protoValidationResponse struct {
	messageType reflect.Type
	logger      log.Logger
}

type protoValidationDiagnostics struct {
	kind, cause   string
	count         int
	fields, rules []string
}

func (i *ProtoValidationInterceptor) reportResponse(method string, response any) {
	registration, ok := i.responses[method]
	if !ok {
		return
	}
	diagnostics := i.checkResponse(registration, response)
	if diagnostics.kind == "" {
		return
	}
	metrics.ResponseValidationFailures.With(i.metricsHandler).Record(1,
		metrics.OperationTag(method), metrics.StringTag("validation_failure_type", diagnostics.kind))
	softassert.Fail(registration.logger, "RPC response failed protobuf validation",
		tag.String("failure_type", diagnostics.kind), tag.String("cause", diagnostics.cause),
		tag.NewInt("violation_count", diagnostics.count), tag.NewStringsTag("field_paths", diagnostics.fields), tag.NewStringsTag("rule_ids", diagnostics.rules))
}

func (i *ProtoValidationInterceptor) checkResponse(registration protoValidationResponse, response any) (diagnostics protoValidationDiagnostics) {
	// A validator bug must not replace an already successful handler result.
	defer func() {
		if recover() != nil {
			diagnostics = protoValidationDiagnostics{kind: "implementation", cause: "validator_panic"}
		}
	}()
	message, ok := response.(proto.Message)
	if !ok || !message.ProtoReflect().IsValid() {
		return protoValidationDiagnostics{kind: "implementation", cause: "missing_or_invalid_response"}
	}
	if reflect.TypeOf(response) != registration.messageType {
		return protoValidationDiagnostics{kind: "implementation", cause: "wrong_response_type"}
	}
	err := i.validate(message)
	if err == nil {
		return diagnostics
	}
	failure, ok := errors.AsType[*protovalidate.ValidationError](err)
	if !ok {
		return protoValidationDiagnostics{kind: "implementation", cause: "validator_error"}
	}
	diagnostics.kind = "violation"
	diagnostics.count = len(failure.Violations)
	for _, item := range failure.Violations {
		if len(diagnostics.fields) == maxProtoValidationDiagnostics {
			break
		}
		diagnostics.fields = append(diagnostics.fields, protoValidationCollectionIndex.ReplaceAllString(protovalidate.FieldPathString(item.Proto.GetField()), "[*]"))
		diagnostics.rules = append(diagnostics.rules, item.Proto.GetRuleId())
	}
	return diagnostics
}
