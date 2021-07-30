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

package client

import (
	elastic6 "github.com/olivere/elastic"
	"github.com/olivere/elastic/v7"
)

type (
	bulkProcessorV6 struct {
		esBulkProcessor *elastic6.BulkProcessor
	}
)

func newBulkProcessorV6(esBulkProcessor *elastic6.BulkProcessor) *bulkProcessorV6 {
	if esBulkProcessor == nil {
		return nil
	}
	return &bulkProcessorV6{
		esBulkProcessor: esBulkProcessor,
	}
}

func (p *bulkProcessorV6) Stop() error {
	errF := p.esBulkProcessor.Flush()
	errS := p.esBulkProcessor.Stop()
	if errF != nil {
		return errF
	}
	return errS
}

func (p *bulkProcessorV6) Add(request *BulkableRequest) {
	switch request.RequestType {
	case BulkableRequestTypeIndex:
		bulkIndexRequest := elastic.NewBulkIndexRequest().
			Index(request.Index).
			Type(docTypeV6).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version).
			Doc(request.Doc)
		p.esBulkProcessor.Add(bulkIndexRequest)
	case BulkableRequestTypeDelete:
		bulkDeleteRequest := elastic.NewBulkDeleteRequest().
			Index(request.Index).
			Type(docTypeV6).
			Id(request.ID).
			VersionType(versionTypeExternal).
			Version(request.Version)
		p.esBulkProcessor.Add(bulkDeleteRequest)
	}
}

// =============== V6/V7 adapters ===============

func convertV7BeforeFuncToV6(beforeFunc elastic.BulkBeforeFunc) elastic6.BulkBeforeFunc {
	if beforeFunc == nil {
		return nil
	}
	return func(executionId int64, requests []elastic6.BulkableRequest) {
		beforeFunc(executionId, convertV6BulkableRequestsToV7(requests))
	}
}

func convertV6BulkableRequestsToV7(requests []elastic6.BulkableRequest) []elastic.BulkableRequest {
	if requests == nil {
		return nil
	}
	requestsV7 := make([]elastic.BulkableRequest, len(requests))
	for i, request := range requests {
		requestsV7[i] = request
	}
	return requestsV7
}

func convertV7AfterFuncToV6(afterFunc elastic.BulkAfterFunc) elastic6.BulkAfterFunc {
	if afterFunc == nil {
		return nil
	}
	return func(executionId int64, requests []elastic6.BulkableRequest, response *elastic6.BulkResponse, err error) {
		errV7 := convertV6ErrorToV7(err)
		afterFunc(
			executionId,
			convertV6BulkableRequestsToV7(requests),
			convertV6BulkableResponseToV7(response),
			errV7)
	}
}

func convertV6ErrorToV7(err error) error {
	if err == nil {
		return nil
	}
	switch err := err.(type) {
	case *elastic6.Error:
		return &elastic.Error{
			Status:  err.Status,
			Details: convertV6ErrorDetailsToV7(err.Details),
		}
	case *elastic.Error:
		return err
	}
	return err
}

func convertV6ErrorDetailsToV7(details *elastic6.ErrorDetails) *elastic.ErrorDetails {
	if details == nil {
		return nil
	}
	return &elastic.ErrorDetails{
		Type:         details.Type,
		Reason:       details.Reason,
		ResourceType: details.ResourceType,
		ResourceId:   details.ResourceId,
		Index:        details.Index,
		Phase:        details.Phase,
		Grouped:      details.Grouped,
		CausedBy:     details.CausedBy,
		RootCause:    convertV6ErrorDetailsSliceToV7(details.RootCause),
		FailedShards: details.FailedShards,
	}
}

func convertV6ErrorDetailsSliceToV7(details []*elastic6.ErrorDetails) []*elastic.ErrorDetails {
	if details == nil {
		return nil
	}
	detailsV7 := make([]*elastic.ErrorDetails, len(details))
	for i, detail := range details {
		detailsV7[i] = convertV6ErrorDetailsToV7(detail)
	}
	return detailsV7
}

func convertV6BulkableResponseToV7(response *elastic6.BulkResponse) *elastic.BulkResponse {
	if response == nil {
		return nil
	}
	return &elastic.BulkResponse{
		Took:   response.Took,
		Errors: response.Errors,
		Items:  convertV6BulkResponseItemMapsToV7(response.Items),
	}
}

func convertV6BulkResponseItemMapsToV7(items []map[string]*elastic6.BulkResponseItem) []map[string]*elastic.BulkResponseItem {
	if items == nil {
		return nil
	}
	itemsV7 := make([]map[string]*elastic.BulkResponseItem, len(items))
	for i, item := range items {
		itemsV7[i] = convertV6BulkResponseItemMapToV7(item)
	}
	return itemsV7
}

func convertV6BulkResponseItemMapToV7(item map[string]*elastic6.BulkResponseItem) map[string]*elastic.BulkResponseItem {
	if item == nil {
		return nil
	}
	itemV7 := make(map[string]*elastic.BulkResponseItem, len(item))
	for k, v := range item {
		itemV7[k] = convertV6BulkResponseItemToV7(v)
	}
	return itemV7
}

func convertV6BulkResponseItemToV7(item *elastic6.BulkResponseItem) *elastic.BulkResponseItem {
	if item == nil {
		return nil
	}
	return &elastic.BulkResponseItem{
		Index:         item.Index,
		Type:          item.Type,
		Id:            item.Id,
		Version:       item.Version,
		Result:        item.Result,
		SeqNo:         item.SeqNo,
		PrimaryTerm:   item.PrimaryTerm,
		Status:        item.Status,
		ForcedRefresh: item.ForcedRefresh,
	}
}
