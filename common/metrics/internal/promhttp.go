package internal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/multierr"
)

var (
	errReadMetricsPage = errors.New("unable to read Prometheus /metrics endpoint")
)

// PrintMetricsPage writes the Prometheus /metrics page to the given file. This is used for testing Prometheus metrics
// that we emit from the server.
func PrintMetricsPage(pr *prometheus.Registry, file io.ReaderFrom) (err error) {
	response := readMetricsPage(pr)
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: expected status OK, got %v", errReadMetricsPage, response.Status)
	}
	defer func() {
		err = multierr.Append(err, response.Body.Close())
	}()
	_, err = file.ReadFrom(response.Body)
	return nil
}

func readMetricsPage(pr *prometheus.Registry) *http.Response {
	handler := promhttp.HandlerFor(pr, promhttp.HandlerOpts{})

	var buffer bytes.Buffer
	recorder := httptest.ResponseRecorder{
		Body: &buffer,
	}
	handler.ServeHTTP(&recorder, &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Path: "/metrics"},
	})

	response := recorder.Result()

	return response
}
