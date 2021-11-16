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
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	elastic6 "github.com/olivere/elastic"
	"github.com/olivere/elastic/v7"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/log"
)

type (
	// clientV6 implements Client
	clientV6 struct {
		esClient *elastic6.Client
	}
)

var _ Client = (*clientV6)(nil)

// newClientV6 create a ES client
func newClientV6(cfg *Config, httpClient *http.Client, logger log.Logger) (*clientV6, error) {
	options := []elastic6.ClientOptionFunc{
		elastic6.SetURL(cfg.URL.String()),
		elastic6.SetBasicAuth(cfg.Username, cfg.Password),
		// Disable healthcheck to prevent blocking client creation (and thus Temporal server startup) if the Elasticsearch is down.
		elastic6.SetHealthcheck(false),
		elastic6.SetSniff(cfg.EnableSniff),
		elastic6.SetRetrier(elastic6.NewBackoffRetrier(elastic6.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond))),
		// Critical to ensure decode of int64 won't lose precision.
		elastic6.SetDecoder(&elastic6.NumberDecoder{}),
	}

	options = append(options, getLoggerOptionsV6(cfg.LogLevel, logger)...)

	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	// TODO (alex): Remove this when https://github.com/olivere/elastic/pull/1507 is merged.
	if cfg.CloseIdleConnectionsInterval != time.Duration(0) {
		if cfg.CloseIdleConnectionsInterval < minimumCloseIdleConnectionsInterval {
			cfg.CloseIdleConnectionsInterval = minimumCloseIdleConnectionsInterval
		}
		go func(interval time.Duration, httpClient *http.Client) {
			closeTimer := time.NewTimer(interval)
			defer closeTimer.Stop()
			for {
				<-closeTimer.C
				closeTimer.Reset(interval)
				httpClient.CloseIdleConnections()
			}
		}(cfg.CloseIdleConnectionsInterval, httpClient)
	}

	options = append(options, elastic6.SetHttpClient(httpClient))

	client, err := elastic6.NewClient(options...)
	if err != nil {
		return nil, convertV6ErrorToV7(err)
	}

	// Enable healthcheck (if configured) after client is successfully created.
	if cfg.EnableHealthcheck {
		client.Stop()
		err = elastic6.SetHealthcheck(true)(client)
		if err != nil {
			return nil, err
		}
		client.Start()
	}

	return &clientV6{esClient: client}, nil
}

func (c *clientV6) Search(ctx context.Context, p *SearchParameters) (*elastic.SearchResult, error) {
	searchSource := elastic6.NewSearchSource().
		Query(p.Query).
		SortBy(convertV7SortersToV6(p.Sorter)...)

	if p.PageSize != 0 {
		searchSource.Size(p.PageSize)
	}

	if len(p.SearchAfter) != 0 {
		searchSource.SearchAfter(p.SearchAfter...)
	}

	searchResult, err := c.esClient.Search(p.Index).SearchSource(searchSource).Do(ctx)
	if err != nil {
		return nil, convertV6ErrorToV7(err)
	}

	return convertV6SearchResultToV7(searchResult), nil
}

func (c *clientV6) OpenScroll(ctx context.Context, p *SearchParameters, keepAliveInterval string) (*elastic.SearchResult, error) {
	scrollService := elastic6.NewScrollService(c.esClient).
		Index(p.Index).
		Query(p.Query).
		SortBy(convertV7SortersToV6(p.Sorter)...).
		KeepAlive(keepAliveInterval)

	if p.PageSize != 0 {
		scrollService.Size(p.PageSize)
	}

	searchResult, err := scrollService.Do(ctx)
	return convertV6SearchResultToV7(searchResult), convertV6ErrorToV7(err)
}

func (c *clientV6) Scroll(ctx context.Context, scrollID string, keepAliveInterval string) (*elastic.SearchResult, error) {
	scrollService := elastic6.NewScrollService(c.esClient)
	result, err := scrollService.ScrollId(scrollID).KeepAlive(keepAliveInterval).Do(ctx)
	return convertV6SearchResultToV7(result), convertV6ErrorToV7(err)
}

func (c *clientV6) CloseScroll(ctx context.Context, id string) error {
	err := elastic6.NewScrollService(c.esClient).ScrollId(id).Clear(ctx)
	return convertV6ErrorToV7(err)
}

func (c *clientV6) Count(ctx context.Context, index string, query elastic.Query) (int64, error) {
	count, err := c.esClient.Count(index).Query(query).Do(ctx)
	return count, convertV6ErrorToV7(err)
}

func (c *clientV6) RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (BulkProcessor, error) {
	esBulkProcessor, err := c.esClient.BulkProcessor().
		Name(p.Name).
		Workers(p.NumOfWorkers).
		BulkActions(p.BulkActions).
		BulkSize(p.BulkSize).
		FlushInterval(p.FlushInterval).
		Backoff(p.Backoff).
		Before(convertV7BeforeFuncToV6(p.BeforeFunc)).
		After(convertV7AfterFuncToV6(p.AfterFunc)).
		Do(ctx)

	return newBulkProcessorV6(esBulkProcessor), convertV6ErrorToV7(err)
}

func (c *clientV6) PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error) {
	body := buildMappingBodyV6(mapping)
	resp, err := c.esClient.PutMapping().Index(index).Type(docTypeV6).BodyJson(body).Do(ctx)
	if err != nil {
		return false, convertV6ErrorToV7(err)
	}
	return resp.Acknowledged, nil
}

func (c *clientV6) WaitForYellowStatus(ctx context.Context, index string) (string, error) {
	resp, err := c.esClient.ClusterHealth().Index(index).WaitForYellowStatus().Do(ctx)
	if err != nil {
		return "", convertV6ErrorToV7(err)
	}
	return resp.Status, nil
}

func (c *clientV6) GetMapping(ctx context.Context, index string) (map[string]string, error) {
	resp, err := c.esClient.GetMapping().Index(index).Type(docTypeV6).Do(ctx)
	if err != nil {
		return nil, convertV6ErrorToV7(err)
	}
	return convertMappingBody(resp, index), nil
}

func (c *clientV6) GetDateFieldType() string {
	return "date"
}

func (c *clientV6) CreateIndex(ctx context.Context, index string) (bool, error) {
	resp, err := c.esClient.CreateIndex(index).Do(ctx)
	if err != nil {
		return false, convertV6ErrorToV7(err)
	}
	return resp.Acknowledged, nil
}

func (c *clientV6) CatIndices(ctx context.Context) (elastic.CatIndicesResponse, error) {
	catIndicesResponse, err := c.esClient.CatIndices().Do(ctx)
	return convertV6CatIndicesResponseToV7(catIndicesResponse), convertV6ErrorToV7(err)
}

func (c *clientV6) Bulk() BulkService {
	return newBulkServiceV6(c.esClient.Bulk())
}

func (c *clientV6) IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error) {
	resp, err := c.esClient.IndexPutTemplate(templateName).BodyString(bodyString).Do(ctx)
	if err != nil {
		return false, convertV6ErrorToV7(err)
	}
	return resp.Acknowledged, nil
}

func (c *clientV6) IndexExists(ctx context.Context, indexName string) (bool, error) {
	exists, err := c.esClient.IndexExists(indexName).Do(ctx)
	return exists, convertV6ErrorToV7(err)
}

func (c *clientV6) DeleteIndex(ctx context.Context, indexName string) (bool, error) {
	resp, err := c.esClient.DeleteIndex(indexName).Do(ctx)
	if err != nil {
		return false, convertV6ErrorToV7(err)
	}
	return resp.Acknowledged, nil
}

func (c *clientV6) IndexPutSettings(ctx context.Context, indexName string, bodyString string) (bool, error) {
	resp, err := c.esClient.IndexPutSettings(indexName).BodyString(bodyString).Do(ctx)
	if err != nil {
		return false, convertV6ErrorToV7(err)
	}
	return resp.Acknowledged, nil
}

func (c *clientV6) IndexGetSettings(ctx context.Context, indexName string) (map[string]*elastic.IndicesGetSettingsResponse, error) {
	resp, err := c.esClient.IndexGetSettings(indexName).Do(ctx)
	return convertV6IndicesGetSettingsResponseMapToV7(resp), convertV6ErrorToV7(err)
}

func (c *clientV6) Delete(ctx context.Context, indexName string, docID string, version int64) error {
	_, err := c.esClient.Delete().
		Index(indexName).
		Id(docID).
		Version(version).
		VersionType(versionTypeExternal).
		Do(ctx)
	return err
}

// =============== V6/V7 adapters ===============

func convertV7SortersToV6(sorters []elastic.Sorter) []elastic6.Sorter {
	sortersV6 := make([]elastic6.Sorter, len(sorters))
	for i, sorter := range sorters {
		sortersV6[i] = sorter
	}
	return sortersV6
}

func convertV6SearchResultToV7(result *elastic6.SearchResult) *elastic.SearchResult {
	if result == nil {
		return nil
	}
	return &elastic.SearchResult{
		Header:          result.Header,
		TookInMillis:    result.TookInMillis,
		TerminatedEarly: result.TerminatedEarly,
		NumReducePhases: result.NumReducePhases,
		Clusters:        convertV6SearchResultClusterToV7(result.Clusters),
		ScrollId:        result.ScrollId,
		Hits:            convertV6SearchHitsToV7(result.Hits),
		TimedOut:        result.TimedOut,
		Error:           convertV6ErrorDetailsToV7(result.Error),
		Status:          result.Status,
		Aggregations:    convertV6AggregationsToV7(result.Aggregations),
		PitId:           "",

		// TODO (alex): these complex structs are not converted. Add conversion funcs before using them in caller code.
		Suggest: nil, // result.Suggest,
		Profile: nil, // result.Profile,
		Shards:  nil, // result.Shards,
	}
}

func convertV6AggregationsToV7(aggregations elastic6.Aggregations) elastic.Aggregations {
	if aggregations == nil {
		return nil
	}
	aggregationsV7 := make(map[string]json.RawMessage, len(aggregations))
	for k, message := range aggregations {
		if message != nil {
			aggregationsV7[k] = *message
		}
	}
	return aggregationsV7
}

func convertV6SearchHitsToV7(hits *elastic6.SearchHits) *elastic.SearchHits {
	if hits == nil {
		return nil
	}
	return &elastic.SearchHits{
		TotalHits: &elastic.TotalHits{
			Value:    hits.TotalHits,
			Relation: "eq",
		},
		MaxScore: hits.MaxScore,
		Hits:     convertV6SearchHitSliceToV7(hits.Hits),
	}
}

func convertV6SearchHitSliceToV7(hits []*elastic6.SearchHit) []*elastic.SearchHit {
	if hits == nil {
		return nil
	}
	hitsV7 := make([]*elastic.SearchHit, len(hits))
	for i, hit := range hits {
		hitsV7[i] = convertV6SearchHitToV7(hit)
	}
	return hitsV7
}

func convertV6SearchHitToV7(hit *elastic6.SearchHit) *elastic.SearchHit {
	if hit == nil {
		return nil
	}
	hitV7 := &elastic.SearchHit{
		Score:          hit.Score,
		Index:          hit.Index,
		Type:           hit.Type,
		Id:             hit.Id,
		Uid:            hit.Uid,
		Routing:        hit.Routing,
		Parent:         hit.Parent,
		Version:        hit.Version,
		SeqNo:          hit.SeqNo,
		PrimaryTerm:    hit.PrimaryTerm,
		Sort:           hit.Sort,
		Highlight:      elastic.SearchHitHighlight(hit.Highlight),
		Fields:         hit.Fields,
		Explanation:    convertV6SearchExplanationToV7(hit.Explanation),
		MatchedQueries: hit.MatchedQueries,
		InnerHits:      convertV6SearchHitInnerHitsMapToV7(hit.InnerHits),
		Nested:         convertV6NestedHitToV7(hit.Nested),
		Shard:          hit.Shard,
		Node:           hit.Node,
	}

	if hit.Source != nil {
		hitV7.Source = *hit.Source
	}

	return hitV7
}

func convertV6NestedHitToV7(nestedHit *elastic6.NestedHit) *elastic.NestedHit {
	if nestedHit == nil {
		return nil
	}
	return &elastic.NestedHit{
		Field:  nestedHit.Field,
		Offset: nestedHit.Offset,
		Child:  convertV6NestedHitToV7(nestedHit.Child),
	}
}

func convertV6SearchHitInnerHitsMapToV7(innerHitsMap map[string]*elastic6.SearchHitInnerHits) map[string]*elastic.SearchHitInnerHits {
	if innerHitsMap == nil {
		return nil
	}
	innerHitsMapV7 := make(map[string]*elastic.SearchHitInnerHits, len(innerHitsMap))
	for k, innerHits := range innerHitsMap {
		innerHitsMapV7[k] = convertV6SearchHitInnerHitsToV7(innerHits)
	}
	return innerHitsMapV7
}

func convertV6SearchHitInnerHitsToV7(innerHits *elastic6.SearchHitInnerHits) *elastic.SearchHitInnerHits {
	if innerHits == nil {
		return nil
	}
	return &elastic.SearchHitInnerHits{
		Hits: convertV6SearchHitsToV7(innerHits.Hits),
	}
}

func convertV6SearchExplanationToV7(explanation *elastic6.SearchExplanation) *elastic.SearchExplanation {
	if explanation == nil {
		return nil
	}
	return &elastic.SearchExplanation{
		Value:       explanation.Value,
		Description: explanation.Description,
		Details:     convertV6SearchExplanationSliceToV7(explanation.Details),
	}
}

func convertV6SearchExplanationSliceToV7(details []elastic6.SearchExplanation) []elastic.SearchExplanation {
	if details == nil {
		return nil
	}
	detailsV7 := make([]elastic.SearchExplanation, len(details))
	for i, detail := range details {
		detailsV7[i] = *convertV6SearchExplanationToV7(&detail)
	}
	return detailsV7
}

func convertV6SearchResultClusterToV7(cluster *elastic6.SearchResultCluster) *elastic.SearchResultCluster {
	if cluster == nil {
		return nil
	}
	return &elastic.SearchResultCluster{
		Successful: cluster.Successful,
		Total:      cluster.Total,
		Skipped:    cluster.Skipped,
	}
}

func convertV6CatIndicesResponseToV7(response elastic6.CatIndicesResponse) elastic.CatIndicesResponse {
	if response == nil {
		return nil
	}
	responseV7 := make(elastic.CatIndicesResponse, len(response))
	for i, row := range response {
		responseV7[i] = convertV6CatIndicesResponseRowToV7(row)
	}
	return responseV7
}

func convertV6CatIndicesResponseRowToV7(row elastic6.CatIndicesResponseRow) elastic.CatIndicesResponseRow {
	return elastic.CatIndicesResponseRow{
		Health:                       row.Health,
		Status:                       row.Status,
		Index:                        row.Index,
		UUID:                         row.UUID,
		Pri:                          row.Pri,
		Rep:                          row.Rep,
		DocsCount:                    row.DocsCount,
		DocsDeleted:                  row.DocsDeleted,
		CreationDate:                 row.CreationDate,
		CreationDateString:           row.CreationDateString,
		StoreSize:                    row.StoreSize,
		PriStoreSize:                 row.PriStoreSize,
		CompletionSize:               row.CompletionSize,
		PriCompletionSize:            row.PriCompletionSize,
		FielddataMemorySize:          row.FielddataMemorySize,
		PriFielddataMemorySize:       row.PriFielddataMemorySize,
		FielddataEvictions:           row.FielddataEvictions,
		PriFielddataEvictions:        row.PriFielddataEvictions,
		QueryCacheMemorySize:         row.QueryCacheMemorySize,
		PriQueryCacheMemorySize:      row.PriQueryCacheMemorySize,
		QueryCacheEvictions:          row.QueryCacheEvictions,
		PriQueryCacheEvictions:       row.PriQueryCacheEvictions,
		RequestCacheMemorySize:       row.RequestCacheMemorySize,
		PriRequestCacheMemorySize:    row.PriRequestCacheMemorySize,
		RequestCacheEvictions:        row.RequestCacheEvictions,
		PriRequestCacheEvictions:     row.PriRequestCacheEvictions,
		RequestCacheHitCount:         row.RequestCacheHitCount,
		PriRequestCacheHitCount:      row.PriRequestCacheHitCount,
		RequestCacheMissCount:        row.RequestCacheMissCount,
		PriRequestCacheMissCount:     row.PriRequestCacheMissCount,
		FlushTotal:                   row.FlushTotal,
		PriFlushTotal:                row.PriFlushTotal,
		FlushTotalTime:               row.FlushTotalTime,
		PriFlushTotalTime:            row.PriFlushTotalTime,
		GetCurrent:                   row.GetCurrent,
		PriGetCurrent:                row.PriGetCurrent,
		GetTime:                      row.GetTime,
		PriGetTime:                   row.PriGetTime,
		GetTotal:                     row.GetTotal,
		PriGetTotal:                  row.PriGetTotal,
		GetExistsTime:                row.GetExistsTime,
		PriGetExistsTime:             row.PriGetExistsTime,
		GetExistsTotal:               row.GetExistsTotal,
		PriGetExistsTotal:            row.PriGetExistsTotal,
		GetMissingTime:               row.GetMissingTime,
		PriGetMissingTime:            row.PriGetMissingTime,
		GetMissingTotal:              row.GetMissingTotal,
		PriGetMissingTotal:           row.PriGetMissingTotal,
		IndexingDeleteCurrent:        row.IndexingDeleteCurrent,
		PriIndexingDeleteCurrent:     row.PriIndexingDeleteCurrent,
		IndexingDeleteTime:           row.IndexingDeleteTime,
		PriIndexingDeleteTime:        row.PriIndexingDeleteTime,
		IndexingDeleteTotal:          row.IndexingDeleteTotal,
		PriIndexingDeleteTotal:       row.PriIndexingDeleteTotal,
		IndexingIndexCurrent:         row.IndexingIndexCurrent,
		PriIndexingIndexCurrent:      row.PriIndexingIndexCurrent,
		IndexingIndexTime:            row.IndexingIndexTime,
		PriIndexingIndexTime:         row.PriIndexingIndexTime,
		IndexingIndexTotal:           row.IndexingIndexTotal,
		PriIndexingIndexTotal:        row.PriIndexingIndexTotal,
		IndexingIndexFailed:          row.IndexingIndexFailed,
		PriIndexingIndexFailed:       row.PriIndexingIndexFailed,
		MergesCurrent:                row.MergesCurrent,
		PriMergesCurrent:             row.PriMergesCurrent,
		MergesCurrentDocs:            row.MergesCurrentDocs,
		PriMergesCurrentDocs:         row.PriMergesCurrentDocs,
		MergesCurrentSize:            row.MergesCurrentSize,
		PriMergesCurrentSize:         row.PriMergesCurrentSize,
		MergesTotal:                  row.MergesTotal,
		PriMergesTotal:               row.PriMergesTotal,
		MergesTotalDocs:              row.MergesTotalDocs,
		PriMergesTotalDocs:           row.PriMergesTotalDocs,
		MergesTotalSize:              row.MergesTotalSize,
		PriMergesTotalSize:           row.PriMergesTotalSize,
		MergesTotalTime:              row.MergesTotalTime,
		PriMergesTotalTime:           row.PriMergesTotalTime,
		RefreshTotal:                 row.RefreshTotal,
		PriRefreshTotal:              row.PriRefreshTotal,
		RefreshExternalTotal:         0,
		PriRefreshExternalTotal:      0,
		RefreshTime:                  row.RefreshTime,
		PriRefreshTime:               row.PriRefreshTime,
		RefreshExternalTime:          "",
		PriRefreshExternalTime:       "",
		RefreshListeners:             row.RefreshListeners,
		PriRefreshListeners:          row.PriRefreshListeners,
		SearchFetchCurrent:           row.SearchFetchCurrent,
		PriSearchFetchCurrent:        row.PriSearchFetchCurrent,
		SearchFetchTime:              row.SearchFetchTime,
		PriSearchFetchTime:           row.PriSearchFetchTime,
		SearchFetchTotal:             row.SearchFetchTotal,
		PriSearchFetchTotal:          row.PriSearchFetchTotal,
		SearchOpenContexts:           row.SearchOpenContexts,
		PriSearchOpenContexts:        row.PriSearchOpenContexts,
		SearchQueryCurrent:           row.SearchQueryCurrent,
		PriSearchQueryCurrent:        row.PriSearchQueryCurrent,
		SearchQueryTime:              row.SearchQueryTime,
		PriSearchQueryTime:           row.PriSearchQueryTime,
		SearchQueryTotal:             row.SearchQueryTotal,
		PriSearchQueryTotal:          row.PriSearchQueryTotal,
		SearchScrollCurrent:          row.SearchScrollCurrent,
		PriSearchScrollCurrent:       row.PriSearchScrollCurrent,
		SearchScrollTime:             row.SearchScrollTime,
		PriSearchScrollTime:          row.PriSearchScrollTime,
		SearchScrollTotal:            row.SearchScrollTotal,
		PriSearchScrollTotal:         row.PriSearchScrollTotal,
		SearchThrottled:              false,
		SegmentsCount:                row.SegmentsCount,
		PriSegmentsCount:             row.PriSegmentsCount,
		SegmentsMemory:               row.SegmentsMemory,
		PriSegmentsMemory:            row.PriSegmentsMemory,
		SegmentsIndexWriterMemory:    row.SegmentsIndexWriterMemory,
		PriSegmentsIndexWriterMemory: row.PriSegmentsIndexWriterMemory,
		SegmentsVersionMapMemory:     row.SegmentsVersionMapMemory,
		PriSegmentsVersionMapMemory:  row.PriSegmentsVersionMapMemory,
		SegmentsFixedBitsetMemory:    row.SegmentsFixedBitsetMemory,
		PriSegmentsFixedBitsetMemory: row.PriSegmentsFixedBitsetMemory,
		WarmerCurrent:                row.WarmerCurrent,
		PriWarmerCurrent:             row.PriWarmerCurrent,
		WarmerTotal:                  row.WarmerTotal,
		PriWarmerTotal:               row.PriWarmerTotal,
		WarmerTotalTime:              row.WarmerTotalTime,
		PriWarmerTotalTime:           row.PriWarmerTotalTime,
		SuggestCurrent:               row.SuggestCurrent,
		PriSuggestCurrent:            row.PriSuggestCurrent,
		SuggestTime:                  row.SuggestTime,
		PriSuggestTime:               row.PriSuggestTime,
		SuggestTotal:                 row.SuggestTotal,
		PriSuggestTotal:              row.PriSuggestTotal,
		MemoryTotal:                  row.MemoryTotal,
		PriMemoryTotal:               row.PriMemoryTotal,
	}
}

func convertV6IndicesGetSettingsResponseMapToV7(response map[string]*elastic6.IndicesGetSettingsResponse) map[string]*elastic.IndicesGetSettingsResponse {
	if response == nil {
		return nil
	}
	responseV7 := make(map[string]*elastic.IndicesGetSettingsResponse, len(response))
	for k, responseItem := range response {
		responseV7[k] = convertV6IndicesGetSettingsResponseToV7(responseItem)
	}
	return responseV7
}

func convertV6IndicesGetSettingsResponseToV7(response *elastic6.IndicesGetSettingsResponse) *elastic.IndicesGetSettingsResponse {
	if response == nil {
		return nil
	}
	return &elastic.IndicesGetSettingsResponse{
		Settings: response.Settings,
	}
}

func getLoggerOptionsV6(logLevel string, logger log.Logger) []elastic6.ClientOptionFunc {
	switch {
	case strings.EqualFold(logLevel, "trace"):
		return []elastic6.ClientOptionFunc{
			elastic6.SetErrorLog(newErrorLogger(logger)),
			elastic6.SetInfoLog(newInfoLogger(logger)),
			elastic6.SetTraceLog(newInfoLogger(logger)),
		}
	case strings.EqualFold(logLevel, "info"):
		return []elastic6.ClientOptionFunc{
			elastic6.SetErrorLog(newErrorLogger(logger)),
			elastic6.SetInfoLog(newInfoLogger(logger)),
		}
	case strings.EqualFold(logLevel, "error"), logLevel == "": // Default is to log errors only.
		return []elastic6.ClientOptionFunc{
			elastic6.SetErrorLog(newErrorLogger(logger)),
		}
	default:
		return nil
	}
}

func buildMappingBodyV6(mapping map[string]enumspb.IndexedValueType) map[string]interface{} {
	properties := make(map[string]interface{}, len(mapping))
	for fieldName, fieldType := range mapping {
		var typeMap map[string]interface{}
		switch fieldType {
		case enumspb.INDEXED_VALUE_TYPE_TEXT:
			typeMap = map[string]interface{}{"type": "text"}
		case enumspb.INDEXED_VALUE_TYPE_KEYWORD:
			typeMap = map[string]interface{}{"type": "keyword"}
		case enumspb.INDEXED_VALUE_TYPE_INT:
			typeMap = map[string]interface{}{"type": "long"}
		case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
			typeMap = map[string]interface{}{
				"type":           "scaled_float",
				"scaling_factor": 10000,
			}
		case enumspb.INDEXED_VALUE_TYPE_BOOL:
			typeMap = map[string]interface{}{"type": "boolean"}
		case enumspb.INDEXED_VALUE_TYPE_DATETIME:
			typeMap = map[string]interface{}{"type": "date"}
		}
		if typeMap != nil {
			properties[fieldName] = typeMap
		}
	}

	body := map[string]interface{}{
		"properties": properties,
	}
	return body
}
