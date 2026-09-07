package searchattribute

import (
	"errors"
	"fmt"
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

var (
	acceptedInvalidValueKeywordList = metrics.NewCounterDef("visibility_accepted_invalid_value_keywordlist")
	invalidListValues               = metrics.NewCounterDef("visibility_invalid_list_values")
)

type (
	// Validator is used to validate search attributes
	Validator struct {
		searchAttributesProvider          Provider
		searchAttributesMapperProvider    MapperProvider
		searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter
		searchAttributesSizeOfValueLimit  dynamicconfig.IntPropertyFnWithNamespaceFilter
		searchAttributesTotalSizeLimit    dynamicconfig.IntPropertyFnWithNamespaceFilter
		visibilityManager                 manager.VisibilityManager

		// allowList allows list of values when it's not keyword list type.
		allowList dynamicconfig.BoolPropertyFnWithNamespaceFilter

		// suppressErrorSetSystemSearchAttribute suppresses errors when the user
		// attempts to set values in system search attributes.
		suppressErrorSetSystemSearchAttribute dynamicconfig.BoolPropertyFnWithNamespaceFilter

		metricsHandler metrics.Handler
		logger         log.Logger
	}
)

// NewValidator create Validator
func NewValidator(
	searchAttributesProvider Provider,
	searchAttributesMapperProvider MapperProvider,
	searchAttributesNumberOfKeysLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	searchAttributesSizeOfValueLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	searchAttributesTotalSizeLimit dynamicconfig.IntPropertyFnWithNamespaceFilter,
	visibilityManager manager.VisibilityManager,
	allowList dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	suppressErrorSetSystemSearchAttribute dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *Validator {
	return &Validator{
		searchAttributesProvider:              searchAttributesProvider,
		searchAttributesMapperProvider:        searchAttributesMapperProvider,
		searchAttributesNumberOfKeysLimit:     searchAttributesNumberOfKeysLimit,
		searchAttributesSizeOfValueLimit:      searchAttributesSizeOfValueLimit,
		searchAttributesTotalSizeLimit:        searchAttributesTotalSizeLimit,
		visibilityManager:                     visibilityManager,
		allowList:                             allowList,
		suppressErrorSetSystemSearchAttribute: suppressErrorSetSystemSearchAttribute,

		metricsHandler: metricsHandler,
		logger:         logger,
	}
}

// Validate search attributes are valid for writing.
// The search attributes must be unaliased before calling validation.
func (v *Validator) Validate(searchAttributes *commonpb.SearchAttributes, namespace string) error {
	if len(searchAttributes.GetIndexedFields()) == 0 {
		return nil
	}

	lengthOfFields := len(searchAttributes.GetIndexedFields())
	if lengthOfFields > v.searchAttributesNumberOfKeysLimit(namespace) {
		return serviceerror.NewInvalidArgumentf(
			"number of search attributes %d exceeds limit %d",
			lengthOfFields,
			v.searchAttributesNumberOfKeysLimit(namespace),
		)
	}

	saTypeMap, err := v.searchAttributesProvider.GetSearchAttributes(
		v.visibilityManager.GetIndexName(),
		false,
	)
	if err != nil {
		return serviceerror.NewUnavailablef(
			"unable to get search attributes from cluster metadata: %v", err,
		)
	}

	saMap := make(map[string]any, len(searchAttributes.GetIndexedFields()))
	for saFieldName, saPayload := range searchAttributes.GetIndexedFields() {
		// user search attribute cannot be a system search attribute
		if _, err = saTypeMap.getType(saFieldName, systemCategory); err == nil {
			if v.suppressErrorSetSystemSearchAttribute(namespace) {
				// if suppressing the error, then just ignore the search attribute
				continue
			}
			return serviceerror.NewInvalidArgumentf(
				"%s attribute can't be set in SearchAttributes", saFieldName,
			)
		}

		saType, err := saTypeMap.getType(saFieldName, customCategory|predefinedCategory)
		if err != nil {
			if errors.Is(err, sadefs.ErrInvalidName) {
				return v.validationError(
					"search attribute %s is not defined",
					saFieldName,
					namespace,
				)
			}
			return v.validationError(
				fmt.Sprintf("unable to get %%s search attribute type: %v", err),
				saFieldName,
				namespace,
			)
		}

		// Don't allow those SA's that are in predefined but not in predefinedWhiteList to be set by a user
		if _, ok := predefined[saFieldName]; ok {
			if _, ok = predefinedWhiteList[saFieldName]; !ok {
				return serviceerror.NewInvalidArgumentf(
					"%s attribute can't be set in SearchAttributes", saFieldName,
				)
			}
		}

		allowList := v.allowList(namespace)

		// Record only invalid usages of list of values:
		// - Any type other than KeywordList
		// - Empty list is acceptable for backwards compatibility to unset a custom search attribute.
		if saType != enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST && isNonEmptyListValues(saPayload) {
			invalidListValues.With(v.metricsHandler).Record(
				1,
				metrics.NamespaceTag(namespace),
				metrics.StringTag("search_attribute_type", saType.String()),
				metrics.StringTag("allow_list", strconv.FormatBool(allowList)),
			)
		}

		saValue, err := sadefs.DecodeValue(saPayload, saType, allowList)
		if err != nil {
			var invalidValue any
			if err = payload.Decode(saPayload, &invalidValue); err != nil {
				invalidValue = fmt.Sprintf("value from <%s>", saPayload.String())
			}
			return v.validationError(
				fmt.Sprintf(
					"invalid value for search attribute %%s of type %s: %v",
					saType,
					invalidValue,
				),
				saFieldName,
				namespace,
			)
		}

		// Log occurrences of invalid values for KeywordList search attribute being accepted.
		// Eg: [nil] is decoded successfully in sadefs.DecodeValue.
		if saType == enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST {
			_, err := sadefs.DecodeKeywordList(saPayload)
			if err != nil {
				var invalidValue any
				if err = payload.Decode(saPayload, &invalidValue); err != nil {
					invalidValue = fmt.Sprintf("value from <%s>", saPayload.String())
				}
				err = v.validationError(
					fmt.Sprintf(
						"invalid value for search attribute %%s of type %s: %v",
						saType,
						invalidValue,
					),
					saFieldName,
					namespace,
				)
				acceptedInvalidValueKeywordList.With(v.metricsHandler).Record(1, metrics.NamespaceTag(namespace))
				v.logger.Warn("invalid KeywordList value decoded successfully", tag.Error(err))
			}
		}

		saMap[saFieldName] = saValue
	}
	_, err = v.visibilityManager.ValidateCustomSearchAttributes(saMap)
	return err
}

// ValidateSize validate search attributes are valid for writing and not exceed limits.
// The search attributes must be unaliased before calling validation.
func (v *Validator) ValidateSize(searchAttributes *commonpb.SearchAttributes, namespace string) error {
	if searchAttributes == nil {
		return nil
	}

	for saFieldName, saPayload := range searchAttributes.GetIndexedFields() {
		if len(saPayload.GetData()) > v.searchAttributesSizeOfValueLimit(namespace) {
			return v.validationError(
				fmt.Sprintf(
					"search attribute %s value size %d exceeds size limit %d",
					"%s",
					len(saPayload.GetData()),
					v.searchAttributesSizeOfValueLimit(namespace),
				),
				saFieldName,
				namespace,
			)
		}
	}

	if searchAttributes.Size() > v.searchAttributesTotalSizeLimit(namespace) {
		return serviceerror.NewInvalidArgumentf(
			"total size of search attributes %d exceeds size limit %d",
			searchAttributes.Size(),
			v.searchAttributesTotalSizeLimit(namespace),
		)
	}

	return nil
}

// Generates a validation error with search attribute alias resolution.
// Input `msg` must contain a single occurrence of `%s` that will be substituted
// by the search attribute alias.
func (v *Validator) validationError(msg string, saFieldName string, namespace string) error {
	saAlias, err := v.getAlias(saFieldName, namespace)
	if err != nil {
		return err
	}
	return serviceerror.NewInvalidArgumentf(msg, saAlias)
}

func (v *Validator) getAlias(saFieldName string, namespaceName string) (string, error) {
	if sadefs.IsMappable(saFieldName) {
		mapper, err := v.searchAttributesMapperProvider.GetMapper(namespace.Name(namespaceName))
		if err != nil {
			return "", err
		}
		if mapper != nil {
			return mapper.GetAlias(saFieldName, namespaceName)
		}
	}
	return saFieldName, nil
}

func isNonEmptyListValues(p *commonpb.Payload) bool {
	if p == nil {
		return false
	}
	if string(p.Metadata[converter.MetadataEncoding]) != converter.MetadataEncodingJSON {
		return false
	}
	return len(p.Data) > 0 && p.Data[0] == '[' && string(p.Data) != "[]"
}
