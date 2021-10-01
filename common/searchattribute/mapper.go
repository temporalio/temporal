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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination mapper_mock.go

package searchattribute

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
)

type (
	// Mapper interface allows overriding custom search attribute names with aliases per namespace.
	// Create an instance of a Mapper interface and pass it to the temporal.NewServer using temporal.WithSearchAttributesMapper.
	// Returned error must be from the serviceerror package.
	Mapper interface {
		GetAlias(fieldName string, namespace string) (string, error)
		GetFieldName(alias string, namespace string) (string, error)
	}
)

// ApplyAliases replaces field names with alias names for custom search attributes.
func ApplyAliases(mapper Mapper, searchAttributes *commonpb.SearchAttributes, namespace string) error {
	if len(searchAttributes.GetIndexedFields()) == 0 || mapper == nil {
		return nil
	}

	newIndexedFields := make(map[string]*commonpb.Payload, len(searchAttributes.GetIndexedFields()))
	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		if !IsMappable(saName) {
			newIndexedFields[saName] = saPayload
			continue
		}

		aliasName, err := mapper.GetAlias(saName, namespace)
		if err != nil {
			if _, isInvalidArgument := err.(*serviceerror.InvalidArgument); isInvalidArgument {
				// Silently ignore serviceerror.InvalidArgument because it indicates unmapped field (alias was deleted, for example).
				// IMPORTANT: ApplyAliases should never return serviceerror.InvalidArgument because it is used by Poll API and the error
				// goes through up to SDK, which shutdowns worker when it receives serviceerror.InvalidArgument as poll response.
				continue
			}
			return err
		}
		newIndexedFields[aliasName] = saPayload
	}

	searchAttributes.IndexedFields = newIndexedFields
	return nil
}

// SubstituteAliases replaces aliases with actual field names for custom search attributes.
func SubstituteAliases(mapper Mapper, searchAttributes *commonpb.SearchAttributes, namespace string) error {
	if len(searchAttributes.GetIndexedFields()) == 0 || mapper == nil {
		return nil
	}

	newIndexedFields := make(map[string]*commonpb.Payload, len(searchAttributes.GetIndexedFields()))
	for saName, saPayload := range searchAttributes.GetIndexedFields() {
		if !IsMappable(saName) {
			newIndexedFields[saName] = saPayload
			continue
		}

		fieldName, err := mapper.GetFieldName(saName, namespace)
		if err != nil {
			return err
		}
		newIndexedFields[fieldName] = saPayload
	}

	searchAttributes.IndexedFields = newIndexedFields
	return nil
}
