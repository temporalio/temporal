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
	"context"
	"time"

	"github.com/olivere/elastic"
	elastic7 "github.com/olivere/elastic/v7"
)

type (
	// clientV6 implements Client
	clientV6 struct {
		esClient *elastic.Client
	}
)

var _ Client = (*clientV6)(nil)

// newClientV6 create a ES client
func newClientV6(config *Config) (*clientV6, error) {
	options := []elastic.ClientOptionFunc{
		elastic.SetURL(config.URL.String()),
		elastic.SetSniff(false),
		elastic.SetBasicAuth(config.Username, config.Password),

		// Disable health check so we don't block client creation (and thus temporal server startup)
		// if the ES instance happens to be down.
		elastic.SetHealthcheck(false),

		elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond))),

		// critical to ensure decode of int64 won't lose precision
		elastic.SetDecoder(&elastic.NumberDecoder{}),
	}

	if config.AWSRequestSigning.Enabled {
		httpClient, err := newAWSElasticsearchHTTPClient(config.AWSRequestSigning)
		if err != nil {
			return nil, err
		}
		options = append(options, elastic.SetHttpClient(httpClient))
	}

	client, err := elastic.NewClient(options...)

	if err != nil {
		return nil, err
	}

	// Re-enable the health check after client has successfully been created.
	client.Stop()
	err = elastic.SetHealthcheck(true)(client)
	if err != nil {
		return nil, err
	}
	client.Start()

	return &clientV6{esClient: client}, nil
}

func (c *clientV6) Search(ctx context.Context, p *SearchParameters) (*elastic7.SearchResult, error) {
	searchService := c.esClient.Search(p.Index).
		Query(p.Query).
		From(p.From).
		SortBy(convertV7SortersToV6(p.Sorter)...)

	if p.PageSize != 0 {
		searchService.Size(p.PageSize)
	}

	if len(p.SearchAfter) != 0 {
		searchService.SearchAfter(p.SearchAfter...)
	}

	searchResult, err := searchService.Do(ctx)
	if err != nil {
		return nil, err
	}

	return convertV6SearchResultToV7(searchResult), nil
}

func convertV7SortersToV6(sorters []elastic7.Sorter) []elastic.Sorter {
	sortersV6 := make([]elastic.Sorter, len(sorters))
	for i, sorter := range sorters {
		sortersV6[i] = sorter
	}
	return sortersV6
}

func convertV6SearchResultToV7(result *elastic.SearchResult) *elastic7.SearchResult {
	if result == nil {
		return nil
	}
	return &elastic7.SearchResult{
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

		// TODO (alex): these complex structs are not converted. Add conversion funcs before using them in caller code.
		Suggest:      nil, // result.Suggest,
		Aggregations: nil, // result.Aggregations,
		Profile:      nil, // result.Profile,
		Shards:       nil, // result.Shards,
	}
}

func convertV6SearchHitsToV7(hits *elastic.SearchHits) *elastic7.SearchHits {
	if hits == nil {
		return nil
	}
	return &elastic7.SearchHits{
		TotalHits: &elastic7.TotalHits{
			Value:    hits.TotalHits,
			Relation: "eq",
		},
		MaxScore: hits.MaxScore,
		Hits:     convertV6SearchHitSliceToV7(hits.Hits),
	}
}

func convertV6SearchHitSliceToV7(hits []*elastic.SearchHit) []*elastic7.SearchHit {
	if hits == nil {
		return nil
	}
	hitsV7 := make([]*elastic7.SearchHit, len(hits))
	for i, hit := range hits {
		hitsV7[i] = convertV6SearchHitToV7(hit)
	}
	return hitsV7
}

func convertV6SearchHitToV7(hit *elastic.SearchHit) *elastic7.SearchHit {
	if hit == nil {
		return nil
	}
	hitV7 := &elastic7.SearchHit{
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
		Highlight:      elastic7.SearchHitHighlight(hit.Highlight),
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

func convertV6NestedHitToV7(nestedHit *elastic.NestedHit) *elastic7.NestedHit {
	if nestedHit == nil {
		return nil
	}
	return &elastic7.NestedHit{
		Field:  nestedHit.Field,
		Offset: nestedHit.Offset,
		Child:  convertV6NestedHitToV7(nestedHit.Child),
	}
}

func convertV6SearchHitInnerHitsMapToV7(innerHitsMap map[string]*elastic.SearchHitInnerHits) map[string]*elastic7.SearchHitInnerHits {
	if innerHitsMap == nil {
		return nil
	}
	innerHitsMapV7 := make(map[string]*elastic7.SearchHitInnerHits, len(innerHitsMap))
	for k, innerHits := range innerHitsMap {
		innerHitsMapV7[k] = convertV6SearchHitInnerHitsToV7(innerHits)
	}
	return innerHitsMapV7
}

func convertV6SearchHitInnerHitsToV7(innerHits *elastic.SearchHitInnerHits) *elastic7.SearchHitInnerHits {
	if innerHits == nil {
		return nil
	}
	return &elastic7.SearchHitInnerHits{
		Hits: convertV6SearchHitsToV7(innerHits.Hits),
	}
}

func convertV6SearchExplanationToV7(explanation *elastic.SearchExplanation) *elastic7.SearchExplanation {
	if explanation == nil {
		return nil
	}
	return &elastic7.SearchExplanation{
		Value:       explanation.Value,
		Description: explanation.Description,
		Details:     convertV6SearchExplanationSliceToV7(explanation.Details),
	}
}

func convertV6SearchExplanationSliceToV7(details []elastic.SearchExplanation) []elastic7.SearchExplanation {
	if details == nil {
		return nil
	}
	detailsV7 := make([]elastic7.SearchExplanation, len(details))
	for i, detail := range details {
		detailsV7[i] = *convertV6SearchExplanationToV7(&detail)
	}
	return detailsV7
}

func convertV6SearchResultClusterToV7(cluster *elastic.SearchResultCluster) *elastic7.SearchResultCluster {
	if cluster == nil {
		return nil
	}
	return &elastic7.SearchResultCluster{
		Successful: cluster.Successful,
		Total:      cluster.Total,
		Skipped:    cluster.Skipped,
	}
}

func (c *clientV6) SearchWithDSL(ctx context.Context, index, query string) (*elastic7.SearchResult, error) {
	searchResult, err := c.esClient.Search(index).Source(query).Do(ctx)
	return convertV6SearchResultToV7(searchResult), err
}

func (c *clientV6) Scroll(ctx context.Context, scrollID string) (*elastic7.SearchResult, ScrollService, error) {
	scrollService := elastic.NewScrollService(c.esClient)
	result, err := scrollService.ScrollId(scrollID).Do(ctx)
	return convertV6SearchResultToV7(result), scrollService, err
}

func (c *clientV6) ScrollFirstPage(ctx context.Context, index, query string) (*elastic7.SearchResult, ScrollService, error) {
	scrollService := elastic.NewScrollService(c.esClient)
	result, err := scrollService.Index(index).Body(query).Do(ctx)
	return convertV6SearchResultToV7(result), scrollService, err
}

func (c *clientV6) Count(ctx context.Context, index, query string) (int64, error) {
	return c.esClient.Count(index).BodyString(query).Do(ctx)
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

	return newBulkProcessorV6(esBulkProcessor), err
}

// root is for nested object like Attr property for search attributes.
func (c *clientV6) PutMapping(ctx context.Context, index, root, key, valueType string) error {
	body := c.buildPutMappingBody(root, key, valueType)
	_, err := c.esClient.PutMapping().Index(index).Type("_doc").BodyJson(body).Do(ctx)
	return err
}

func (c *clientV6) CreateIndex(ctx context.Context, index string) error {
	_, err := c.esClient.CreateIndex(index).Do(ctx)
	return err
}

func (c *clientV6) buildPutMappingBody(root, key, valueType string) map[string]interface{} {
	body := make(map[string]interface{})
	if len(root) != 0 {
		body["properties"] = map[string]interface{}{
			root: map[string]interface{}{
				"properties": map[string]interface{}{
					key: map[string]interface{}{
						"type": valueType,
					},
				},
			},
		}
	} else {
		body["properties"] = map[string]interface{}{
			key: map[string]interface{}{
				"type": valueType,
			},
		}
	}
	return body
}
