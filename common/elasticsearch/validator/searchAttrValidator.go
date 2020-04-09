package validator

import (
	"fmt"

	commonpb "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

// SearchAttributesValidator is used to validate search attributes
type SearchAttributesValidator struct {
	logger log.Logger

	validSearchAttributes             dynamicconfig.MapPropertyFn
	searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter
	searchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithNamespaceFilter
	searchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithNamespaceFilter
}

// NewSearchAttributesValidator create SearchAttributesValidator
func NewSearchAttributesValidator(
	logger log.Logger,
	validSearchAttributes dynamicconfig.MapPropertyFn,
	searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	searchAttributesSizeOfValueLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	searchAttributesTotalSizeLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *SearchAttributesValidator {
	return &SearchAttributesValidator{
		logger:                            logger,
		validSearchAttributes:             validSearchAttributes,
		searchAttributesNumberOfKeysLimit: searchAttributesNumberOfKeysLimit,
		searchAttributesSizeOfValueLimit:  searchAttributesSizeOfValueLimit,
		searchAttributesTotalSizeLimit:    searchAttributesTotalSizeLimit,
	}
}

// ValidateSearchAttributes validate search attributes are valid for writing and not exceed limits
func (sv *SearchAttributesValidator) ValidateSearchAttributes(input *commonpb.SearchAttributes, namespace string) error {
	if input == nil {
		return nil
	}

	// verify: number of keys <= limit
	fields := input.GetIndexedFields()
	lengthOfFields := len(fields)
	if lengthOfFields > sv.searchAttributesNumberOfKeysLimit(namespace) {
		sv.logger.WithTags(tag.Number(int64(lengthOfFields)), tag.WorkflowNamespace(namespace)).
			Error("number of keys in search attributes exceed limit")
		return serviceerror.NewInvalidArgument(fmt.Sprintf("number of keys %d exceed limit", lengthOfFields))
	}

	totalSize := 0
	for key, val := range fields {
		// verify: key is whitelisted
		if !sv.isValidSearchAttributes(key) {
			sv.logger.WithTags(tag.ESKey(key), tag.WorkflowNamespace(namespace)).
				Error("invalid search attribute")
			return serviceerror.NewInvalidArgument(fmt.Sprintf("%s is not valid search attribute", key))
		}
		// verify: key is not system reserved
		if definition.IsSystemIndexedKey(key) {
			sv.logger.WithTags(tag.ESKey(key), tag.WorkflowNamespace(namespace)).
				Error("illegal update of system reserved attribute")
			return serviceerror.NewInvalidArgument(fmt.Sprintf("%s is read-only Temporal reservered attribute", key))
		}
		// verify: size of single value <= limit
		if len(val) > sv.searchAttributesSizeOfValueLimit(namespace) {
			sv.logger.WithTags(tag.ESKey(key), tag.Number(int64(len(val))), tag.WorkflowNamespace(namespace)).
				Error("value size of search attribute exceed limit")
			return serviceerror.NewInvalidArgument(fmt.Sprintf("size limit exceed for key %s", key))
		}
		totalSize += len(key) + len(val)
	}

	// verify: total size <= limit
	if totalSize > sv.searchAttributesTotalSizeLimit(namespace) {
		sv.logger.WithTags(tag.Number(int64(totalSize)), tag.WorkflowNamespace(namespace)).
			Error("total size of search attributes exceed limit")
		return serviceerror.NewInvalidArgument(fmt.Sprintf("total size %d exceed limit", totalSize))
	}

	return nil
}

// isValidSearchAttributes return true if key is registered
func (sv *SearchAttributesValidator) isValidSearchAttributes(key string) bool {
	validAttr := sv.validSearchAttributes()
	_, isValidKey := validAttr[key]
	return isValidKey
}
