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
	// ClientV7 implements Client
	ClientV7 struct {
		EsClient *elastic.Client
		url      url.URL
	}
)

var _ Client = (*ClientV7)(nil)

// NewClientV7 create a ES client
func NewClientV7(cfg *Config, httpClient *http.Client, logger log.Logger) (*ClientV7, error) {
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
		elastic.SetGzip(!cfg.DisableGzip),
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

	return &ClientV7{
		EsClient: client,
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

func (c *ClientV7) Get(ctx context.Context, index string, docID string) (*elastic.GetResult, error) {
	return c.EsClient.Get().Index(index).Id(docID).Do(ctx)
}

func (c *ClientV7) Search(ctx context.Context, p *SearchParameters) (*elastic.SearchResult, error) {
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

	{
		if src, err := searchSource.Source(); err == nil {
			if srcJson, err := json.Marshal(src); err == nil {
				fmt.Printf("[FOO] %s\n", srcJson)
			}
		}
	}

	return c.EsClient.Search(p.Index).
		AllowPartialSearchResults(false).
		SearchSource(searchSource).
		Do(ctx)
}

func (c *ClientV7) Count(ctx context.Context, index string, query elastic.Query) (int64, error) {
	return c.EsClient.Count(index).Query(query).Do(ctx)
}

func (c *ClientV7) CountGroupBy(
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
	return c.EsClient.Search(index).
		AllowPartialSearchResults(false).
		SearchSource(searchSource).
		Do(ctx)
}

func (c *ClientV7) RunBulkProcessor(ctx context.Context, p *BulkProcessorParameters) (BulkProcessor, error) {
	esBulkProcessor, err := c.EsClient.BulkProcessor().
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

func (c *ClientV7) PutMapping(ctx context.Context, index string, mapping map[string]enumspb.IndexedValueType) (bool, error) {
	body := buildMappingBody(mapping)
	resp, err := c.EsClient.PutMapping().Index(index).BodyJson(body).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, err
}

func (c *ClientV7) WaitForYellowStatus(ctx context.Context, index string) (string, error) {
	resp, err := c.EsClient.ClusterHealth().Index(index).WaitForYellowStatus().Do(ctx)
	if err != nil {
		return "", err
	}
	return resp.Status, err
}

func (c *ClientV7) GetMapping(ctx context.Context, index string) (map[string]string, error) {
	// Manually build mapping request because olivere/elastic/v7 client doesn't work with ES8
	path, err := uritemplates.Expand("/{index}/_mapping", map[string]string{
		"index": index,
	})
	if err != nil {
		return nil, err
	}

	// Get HTTP response
	res, err := c.EsClient.PerformRequest(ctx, elastic.PerformRequestOptions{
		Method:  "GET",
		Path:    path,
		Params:  url.Values{},
		Headers: http.Header{},
	})
	if err != nil {
		return nil, err
	}

	// Decode body
	var body map[string]any
	if err := json.Unmarshal(res.Body, &body); err != nil {
		return nil, err
	}

	return convertMappingBody(body, index), nil
}

func (c *ClientV7) GetDateFieldType() string {
	return "date_nanos"
}

func (c *ClientV7) CreateIndex(ctx context.Context, index string, body map[string]any) (bool, error) {
	if body == nil {
		body = make(map[string]any)
	}
	resp, err := c.EsClient.CreateIndex(index).BodyJson(body).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *ClientV7) IsNotFoundError(err error) bool {
	return elastic.IsNotFound(err)
}

func (c *ClientV7) CatIndices(ctx context.Context, target string) (elastic.CatIndicesResponse, error) {
	return c.EsClient.CatIndices().Index(target).Do(ctx)
}

func (c *ClientV7) Bulk() BulkService {
	return newBulkService(c.EsClient.Bulk())
}

func (c *ClientV7) IndexPutTemplate(ctx context.Context, templateName string, bodyString string) (bool, error) {
	//lint:ignore SA1019 Changing to IndexPutIndexTemplate requires template changes and will be done separately.
	resp, err := c.EsClient.IndexPutTemplate(templateName).BodyString(bodyString).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *ClientV7) IndexPutMapping(ctx context.Context, indexName string, bodyString string) (bool, error) {
	// Use raw HTTP request to update index mappings
	path := fmt.Sprintf("/%s/_mapping", indexName)
	resp, err := c.EsClient.PerformRequest(ctx, elastic.PerformRequestOptions{
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

func (c *ClientV7) ClusterPutSettings(ctx context.Context, bodyString string) (bool, error) {
	// Use raw HTTP request since ClusterPutSettings is not available in olivere/elastic v7
	resp, err := c.EsClient.PerformRequest(ctx, elastic.PerformRequestOptions{
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

func (c *ClientV7) IndexExists(ctx context.Context, indexName string) (bool, error) {
	return c.EsClient.IndexExists(indexName).Do(ctx)
}

func (c *ClientV7) DeleteIndex(ctx context.Context, indexName string) (bool, error) {
	resp, err := c.EsClient.DeleteIndex(indexName).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *ClientV7) IndexPutSettings(ctx context.Context, indexName string, bodyString string) (bool, error) {
	resp, err := c.EsClient.IndexPutSettings(indexName).BodyString(bodyString).Do(ctx)
	if err != nil {
		return false, err
	}
	return resp.Acknowledged, nil
}

func (c *ClientV7) IndexGetSettings(ctx context.Context, indexName string) (map[string]*elastic.IndicesGetSettingsResponse, error) {
	return c.EsClient.IndexGetSettings(indexName).Do(ctx)
}

func (c *ClientV7) Delete(ctx context.Context, indexName string, docID string, version int64) error {
	_, err := c.EsClient.Delete().
		Index(indexName).
		Id(docID).
		Version(version).
		VersionType(versionTypeExternal).
		Do(ctx)
	return err
}

// Ping returns whether or not the elastic search cluster is available
func (c *ClientV7) Ping(ctx context.Context) error {
	_, _, err := c.EsClient.Ping(c.url.String()).Do(ctx)
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

func buildMappingBody(mapping map[string]enumspb.IndexedValueType) map[string]any {
	properties := make(map[string]any, len(mapping))
	for fieldName, fieldType := range mapping {
		var typeMap map[string]any
		switch fieldType {
		case enumspb.INDEXED_VALUE_TYPE_TEXT:
			typeMap = map[string]any{"type": "text"}
		case enumspb.INDEXED_VALUE_TYPE_KEYWORD, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
			typeMap = map[string]any{"type": "keyword"}
		case enumspb.INDEXED_VALUE_TYPE_INT:
			typeMap = map[string]any{"type": "long"}
		case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
			typeMap = map[string]any{
				"type":           "scaled_float",
				"scaling_factor": 10000,
			}
		case enumspb.INDEXED_VALUE_TYPE_BOOL:
			typeMap = map[string]any{"type": "boolean"}
		case enumspb.INDEXED_VALUE_TYPE_DATETIME:
			typeMap = map[string]any{"type": "date_nanos"}
		}
		if typeMap != nil {
			properties[fieldName] = typeMap
		}
	}

	body := map[string]any{
		"properties": properties,
	}
	return body
}

func convertMappingBody(esMapping map[string]any, indexName string) map[string]string {
	result := make(map[string]string)
	index, ok := esMapping[indexName]
	if !ok {
		return result
	}
	indexMap, ok := index.(map[string]any)
	if !ok {
		return result
	}
	mappings, ok := indexMap["mappings"]
	if !ok {
		return result
	}
	mappingsMap, ok := mappings.(map[string]any)
	if !ok {
		return result
	}

	properties, ok := mappingsMap["properties"]
	if !ok {
		return result
	}
	propMap, ok := properties.(map[string]any)
	if !ok {
		return result
	}

	for fieldName, fieldProp := range propMap {
		fieldPropMap, ok := fieldProp.(map[string]any)
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
