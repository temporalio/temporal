package versioninfo

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// RequestTimeout bounds a single version-info call. The check is advisory
// background work on a 24h cadence, so it is better for it to give up quickly
// than to hold a goroutine and a connection waiting on an endpoint that
// accepted the request and then went quiet.
const RequestTimeout = 10 * time.Second

type Caller struct {
	Scheme string
	Host   string
}

func NewCaller() Caller {
	return Caller{"https", "version-info.temporal.io"}
}

// Call performs the version check. The context bounds the whole exchange,
// including reading the response body: a server that sends headers and then
// stalls mid-body would otherwise still hang the caller.
func (c Caller) Call(ctx context.Context, r *VersionCheckRequest) (*VersionCheckResponse, error) {
	err := validateRequest(r)
	if err != nil {
		return nil, err
	}
	u := c.getUrl(r)
	tr := &http.Transport{
		DisableKeepAlives:   true,
		MaxIdleConnsPerHost: -1,
	}
	if c.Scheme == "https" {
		tr.TLSClientConfig = &tls.Config{}
	}
	// Timeout as well as the context: it covers the case where a caller passes a
	// context with no deadline, and unlike the context it also bounds the body read.
	client := &http.Client{Transport: tr, Timeout: RequestTimeout}
	reqBody, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("bad response code %v", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	versionCheckResponse := &VersionCheckResponse{}
	if err := json.Unmarshal(body, versionCheckResponse); err != nil {
		return nil, err
	}
	if err := validateResponse(versionCheckResponse); err != nil {
		return nil, err
	}
	return versionCheckResponse, nil
}

func validateResponse(r *VersionCheckResponse) error {
	if len(r.Products) == 0 {
		return errors.New("invalid response: missing product list")
	}
	firstProduct := r.Products[0]
	if firstProduct.Product == "" || firstProduct.Current.Version == "" || firstProduct.Recommended.Version == "" {
		return errors.New("invalid response: missing product name, current or recommended version")
	}
	return nil
}
func validateRequest(r *VersionCheckRequest) error {
	if r.Product == "" || r.Version == "" || r.ClusterID == "" || r.DB == "" || r.OS == "" || r.Arch == "" || r.Timestamp == 0 {
		return errors.New("invalid request: missing required fields")
	}
	for _, info := range r.SDKInfo {
		if info.Name == "" || info.Version == "" {
			return errors.New("invalid request: missing required fields")
		}
	}
	return nil
}

func (c Caller) getUrl(r *VersionCheckRequest) *url.URL {
	var u url.URL
	u.Scheme = c.Scheme
	u.Host = c.Host
	u.Path = "check"
	return &u
}
