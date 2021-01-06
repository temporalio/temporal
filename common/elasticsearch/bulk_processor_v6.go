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

package elasticsearch

import (
	"github.com/olivere/elastic"
	elastic7 "github.com/olivere/elastic/v7"
)

type (
	bulkProcessorV6 struct {
		esBulkProcessor *elastic.BulkProcessor
	}
)

func newBulkProcessorV6(esBulkProcessor *elastic.BulkProcessor) *bulkProcessorV6 {
	if esBulkProcessor == nil {
		return nil
	}
	return &bulkProcessorV6{
		esBulkProcessor: esBulkProcessor,
	}
}

func (p *bulkProcessorV6) Stop() error {
	return p.esBulkProcessor.Stop()
}

func (p *bulkProcessorV6) Add(request elastic7.BulkableRequest) {
	p.esBulkProcessor.Add(request)
}

func convertV7BeforeFuncToV6(beforeFunc elastic7.BulkBeforeFunc) elastic.BulkBeforeFunc {
	if beforeFunc == nil {
		return nil
	}
	return func(executionId int64, requests []elastic.BulkableRequest) {
		beforeFunc(executionId, convertV6BulkableRequestsToV7(requests))
	}
}

func convertV6BulkableRequestsToV7(requests []elastic.BulkableRequest) []elastic7.BulkableRequest {
	if requests == nil {
		return nil
	}
	requestsV7 := make([]elastic7.BulkableRequest, len(requests))
	for i, request := range requests {
		requestsV7[i] = request
	}
	return requestsV7
}

func convertV7AfterFuncToV6(afterFunc elastic7.BulkAfterFunc) elastic.BulkAfterFunc {
	if afterFunc == nil {
		return nil
	}
	return func(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
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
	case *elastic.Error:
		return &elastic7.Error{
			Status:  err.Status,
			Details: convertV6ErrorDetailsToV7(err.Details),
		}
	case *elastic7.Error:
		return err
	}
	return err
}

func convertV6ErrorDetailsToV7(details *elastic.ErrorDetails) *elastic7.ErrorDetails {
	if details == nil {
		return nil
	}
	return &elastic7.ErrorDetails{
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

func convertV6ErrorDetailsSliceToV7(details []*elastic.ErrorDetails) []*elastic7.ErrorDetails {
	if details == nil {
		return nil
	}
	detailsV7 := make([]*elastic7.ErrorDetails, len(details))
	for i, detail := range details {
		detailsV7[i] = convertV6ErrorDetailsToV7(detail)
	}
	return detailsV7
}

func convertV6BulkableResponseToV7(response *elastic.BulkResponse) *elastic7.BulkResponse {
	if response == nil {
		return nil
	}
	return &elastic7.BulkResponse{
		Took:   response.Took,
		Errors: response.Errors,
		Items:  convertV6BulkResponseItemMapsToV7(response.Items),
	}
}

func convertV6BulkResponseItemMapsToV7(items []map[string]*elastic.BulkResponseItem) []map[string]*elastic7.BulkResponseItem {
	if items == nil {
		return nil
	}
	itemsV7 := make([]map[string]*elastic7.BulkResponseItem, len(items))
	for i, item := range items {
		itemsV7[i] = convertV6BulkResponseItemMapToV7(item)
	}
	return itemsV7
}

func convertV6BulkResponseItemMapToV7(item map[string]*elastic.BulkResponseItem) map[string]*elastic7.BulkResponseItem {
	if item == nil {
		return nil
	}
	itemV7 := make(map[string]*elastic7.BulkResponseItem, len(item))
	for k, v := range item {
		itemV7[k] = convertV6BulkResponseItemToV7(v)
	}
	return itemV7
}

func convertV6BulkResponseItemToV7(item *elastic.BulkResponseItem) *elastic7.BulkResponseItem {
	if item == nil {
		return nil
	}
	return &elastic7.BulkResponseItem{
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
