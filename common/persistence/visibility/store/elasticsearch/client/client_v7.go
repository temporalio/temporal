package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/olivere/elastic/v7/uritemplates"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/log"
)

type (
	// clientImpl implements Client
	clientImpl struct {
		esClient *elastic.Client
		url      url.URL
	}
)

var _ Client = (*clientImpl)(nil)

// newClient create a ES client
func newClient(cfg *Config, httpClient *http.Client, logger log.Logger) (*clientImpl, error) {
	var urls []string
	if len(cfg.URLs) > 0 {
		urls = make([]string, len(cfg.URLs))
		for i, u := range cfg.URLs {
			urls[i] = u.String()
		}
	} else {
		urls = []string{cfg.URL.String()}
	}
	options := []elastic.ClientOptionFunc{
		elastic.SetURL(urls...),
		elastic.SetBasicAuth(cfg.Username, cfg.Password),
		// Disable healthcheck to prevent blocking client creation (and thus Temporal server startup) if the Elasticsearch is down.
		elastic.SetHealthcheck(false),
		elastic.SetSniff(cfg.EnableSniff),
		elastic.SetRetrier(elastic.NewBackoffRetrier(elastic.NewExponentialBackoff(128*time.Millisecond, 513*time.Millisecond))),
		// Critical to ensure decode of int64 won't lose precision.
		elastic.SetDecoder(&elastic.NumberDecoder{}),
		elastic.SetGzip(true),
	}

	options = append(options, getLoggerOptions(cfg.LogLevel, logger)...)

	if httpClient == nil {
		// Check if httpClient is set in config (e.g., AWS HTTP client)
		if configHTTPClient := cfg.GetHttpClient(); configHTTPClient != nil {
			httpClient = configHTTPClient
		} else if cfg.TLS != nil && cfg.TLS.Enabled {
			tlsHttpClient, err := buildTLSHTTPClient(cfg.TLS)
			if err != nil {
				return nil, fmt.Errorf("unable to create TLS HTTP client: %w", err)
			}
			httpClient = tlsHttpClient
		} else {
			httpClient = http.DefaultClient
		}
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

	options = append(options, elastic.SetHttpClient(httpClient))

	client, err := elastic.NewClient(options...)
	if err != nil {
		return nil, err
	}

	// Enable healthcheck (if configured) after client is successfully created.
	if cfg.EnableHealthcheck {
		client.Stop()
		err = elastic.SetHealthcheck(true)(client)
		if err != nil {
			return nil, err
		}
		client.Start()
	}

	return &clientImpl{
		esClient: client,
		url:      cfg.URL,
	}, nil
}

// Build Http Client with TLS
func buildTLSHTTPClient(config *auth.TLS) (*http.Client, error) {
	tlsConfig, err := auth.NewTLSConfig(config)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	tlsClient := &http.Client{Transport: transport}

	return tlsClient, nil
}

func (c *clientImpl) Get(ctx context.Context, index string, docID string) (*elastic.GetResult, error) {
	return c.esClient.Get().Index(index).Id(docID).Do(ctx)
}

func (c *clientImpl) Search(ctx context.Context, p *SearchParameters) (*elastic.SearchResult, error) {
	searchSource := elastic.NewSearchSource().
		Query(p.Query).
		SortBy(p.Sorter...).
		TrackTotalHits(false)

	if p.PageSize != 0 {
		searchSource.Size(p.PageSize)
	}

	if len(p.SearchAfter) != 0 {
		searchSource.SearchAfter(p.SearchAfter...)
	}

	return c.esClient.Search(p.Index).SearchSource(searchSource).Do(ctx)
}

func (c *clientImpl) Count(ctx context.Context, index string, query elastic.Query) (int64, error) {
	return c.esClient.Count(index).Query(query).Do(ctx)
}

func (c *clientImpl) CountGroupBy(
	ctx context.Context,
	index string,
	query elastic.Query,
	aggName string,
	agg elastic.Aggregation,
) (*elastic.SearchResult, error) {
	searchSource := elastic.NewSearchSource().
		Query(query).
		Size(0).
		TrackTotalHits(false).
		Aggregation(aggName, agg)
	return c.esClient.Search(index).SearchSource(searchSource).Do(ctx)
}

func (c *clientImpl) RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (BulkProcessor, error) {
	esBulkProcessor, err := c.esClient.BulkProcessor().
		Name(p.Name).
		Workers(p.NumOfWorkers).
		BulkActions(p.BulkActions).
		BulkSize(p.BulkSize).
		FlushInterval(p.FlushInterval).
		Before(p.BeforeFunc).
		After(p.AfterFunc).
		// Disable built-in retry logic because visibility task processor has its own.
		RetryItemStatusCodes().
		Do(ctx)

	return newBulkProcessor(esBulkProcessor), err
}

func (c *clientImpl) PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error) {
	body := buildMappingBody(mapping)
	resp, err := c.esClient.PutMapping().Index(index).BodyJson(body).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, err
}

func (c *clientImpl) WaitForYellowStatus(ctx context.Context, index string) (string, error) {
	resp, err := c.esClient.ClusterHealth().Index(index).WaitForYellowStatus().Do(ctx)
	if err != nil {
		return "", err
	}
	return resp.Status, err
}

func (c *clientImpl) GetMapping(ctx context.Context, index string) (map[string]string, error) {
	// Manually build mapping request because olivere/elastic/v7 client doesn't work with ES8
	path, err := uritemplates.Expand("/{index}/_mapping", map[string]string{
		"index": index,
	})
	if err != nil {
		return nil, err
	}

	// Get HTTP response
	res, err := c.esClient.PerformRequest(ctx, elastic.PerformRequestOptions{
		Method:  "GET",
		Path:    path,
		Params:  url.Values{},
		Headers: http.Header{},
	})
	if err != nil {
		return nil, err
	}

	// Decode body
	var body map[string]interface{}
	if err := json.Unmarshal(res.Body, &body); err != nil {
		return nil, err
	}

	return convertMappingBody(body, index), nil
}

func (c *clientImpl) GetDateFieldType() string {
	return "date_nanos"
}

func (c *clientImpl) CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error) {
	if body == nil {
		body = make(map[string]interface{})
	}
	resp, err := c.esClient.CreateIndex(index).BodyJson(body).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *clientImpl) IsNotFoundError(err error) bool {
	return elastic.IsNotFound(err)
}

func (c *clientImpl) CatIndices(ctx context.Context, target string) (elastic.CatIndicesResponse, error) {
	return c.esClient.CatIndices().Index(target).Do(ctx)
}

func (c *clientImpl) Bulk() BulkService {
	return newBulkService(c.esClient.Bulk())
}

func (c *clientImpl) IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error) {
	//lint:ignore SA1019 Changing to IndexPutIndexTemplate requires template changes and will be done separately.
	resp, err := c.esClient.IndexPutTemplate(templateName).BodyString(bodyString).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *clientImpl) IndexPutMapping(ctx context.Context, indexName string, bodyString string) (bool, error) {
	// Use raw HTTP request to update index mappings
	path := fmt.Sprintf("/%s/_mapping", indexName)
	resp, err := c.esClient.PerformRequest(ctx, elastic.PerformRequestOptions{
		Method:      "PUT",
		Path:        path,
		Body:        bodyString,
		ContentType: "application/json",
	})
	if err != nil {
		return false, err
	}

	// Parse the response to check if it was acknowledged
	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return false, err
	}

	return result.Acknowledged, nil
}

func (c *clientImpl) ClusterPutSettings(ctx context.Context, bodyString string) (bool, error) {
	// Use raw HTTP request since ClusterPutSettings is not available in olivere/elastic v7
	resp, err := c.esClient.PerformRequest(ctx, elastic.PerformRequestOptions{
		Method:      "PUT",
		Path:        "/_cluster/settings",
		Body:        bodyString,
		ContentType: "application/json",
	})
	if err != nil {
		return false, err
	}

	// Parse the response to check if it was acknowledged
	var result struct {
		Acknowledged bool `json:"acknowledged"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return false, err
	}

	return result.Acknowledged, nil
}

func (c *clientImpl) IndexExists(ctx context.Context, indexName string) (bool, error) {
	return c.esClient.IndexExists(indexName).Do(ctx)
}

func (c *clientImpl) DeleteIndex(ctx context.Context, indexName string) (bool, error) {
	resp, err := c.esClient.DeleteIndex(indexName).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *clientImpl) IndexPutSettings(ctx context.Context, indexName string, bodyString string) (bool, error) {
	resp, err := c.esClient.IndexPutSettings(indexName).BodyString(bodyString).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *clientImpl) IndexGetSettings(ctx context.Context, indexName string) (map[string]*elastic.IndicesGetSettingsResponse, error) {
	return c.esClient.IndexGetSettings(indexName).Do(ctx)
}

func (c *clientImpl) Delete(ctx context.Context, indexName string, docID string, version int64) error {
	_, err := c.esClient.Delete().
		Index(indexName).
		Id(docID).
		Version(version).
		VersionType(versionTypeExternal).
		Do(ctx)
	return err
}

// Ping returns whether or not the elastic search cluster is available
func (c *clientImpl) Ping(ctx context.Context) error {
	_, _, err := c.esClient.Ping(c.url.String()).Do(ctx)
	return err
}

func getLoggerOptions(logLevel string, logger log.Logger) []elastic.ClientOptionFunc {
	switch {
	case strings.EqualFold(logLevel, "trace"):
		return []elastic.ClientOptionFunc{
			elastic.SetErrorLog(newErrorLogger(logger)),
			elastic.SetInfoLog(newInfoLogger(logger)),
			elastic.SetTraceLog(newInfoLogger(logger)),
		}
	case strings.EqualFold(logLevel, "info"):
		return []elastic.ClientOptionFunc{
			elastic.SetErrorLog(newErrorLogger(logger)),
			elastic.SetInfoLog(newInfoLogger(logger)),
		}
	case strings.EqualFold(logLevel, "error"), logLevel == "": // Default is to log errors only.
		return []elastic.ClientOptionFunc{
			elastic.SetErrorLog(newErrorLogger(logger)),
		}
	default:
		return nil
	}
}

func buildMappingBody(mapping map[string]enumspb.IndexedValueType) map[string]interface{} {
	properties := make(map[string]interface{}, len(mapping))
	for fieldName, fieldType := range mapping {
		var typeMap map[string]interface{}
		switch fieldType {
		case enumspb.INDEXED_VALUE_TYPE_TEXT:
			typeMap = map[string]interface{}{"type": "text"}
		case enumspb.INDEXED_VALUE_TYPE_KEYWORD, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
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
			typeMap = map[string]interface{}{"type": "date_nanos"}
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

func convertMappingBody(esMapping map[string]interface{}, indexName string) map[string]string {
	result := make(map[string]string)
	index, ok := esMapping[indexName]
	if !ok {
		return result
	}
	indexMap, ok := index.(map[string]interface{})
	if !ok {
		return result
	}
	mappings, ok := indexMap["mappings"]
	if !ok {
		return result
	}
	mappingsMap, ok := mappings.(map[string]interface{})
	if !ok {
		return result
	}

	properties, ok := mappingsMap["properties"]
	if !ok {
		return result
	}
	propMap, ok := properties.(map[string]interface{})
	if !ok {
		return result
	}

	for fieldName, fieldProp := range propMap {
		fieldPropMap, ok := fieldProp.(map[string]interface{})
		if !ok {
			continue
		}
		tYpe, ok := fieldPropMap["type"]
		if !ok {
			continue
		}
		typeStr, ok := tYpe.(string)
		if !ok {
			continue
		}
		result[fieldName] = typeStr
	}

	return result
}
