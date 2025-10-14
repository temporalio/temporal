// Package nexusrpc provides client and server implementations of the Nexus [HTTP API]
//
// [HTTP API]: https://github.com/nexus-rpc/api
package nexusrpc

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
)

const (
	// Nexus specific headers.
	headerOperationState     = "nexus-operation-state"
	headerRequestID          = "nexus-request-id"
	headerLink               = "nexus-link"
	headerOperationStartTime = "nexus-operation-start-time"
	headerOperationCloseTime = "nexus-operation-close-time"
	headerRetryable          = "nexus-request-retryable"
)

const contentTypeJSON = "application/json"

// Query param for passing a callback URL.
const queryCallbackURL = "callback"

// HTTP status code for failed operation responses.
const statusOperationFailed = http.StatusFailedDependency

func isMediaTypeJSON(contentType string) bool {
	if contentType == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && mediaType == "application/json"
}

func prefixStrippedHTTPHeaderToNexusHeader(httpHeader http.Header, prefix string) nexus.Header {
	header := nexus.Header{}
	for k, v := range httpHeader {
		lowerK := strings.ToLower(k)
		if strings.HasPrefix(lowerK, prefix) {
			// Nexus headers can only have single values, ignore multiple values.
			header[lowerK[len(prefix):]] = v[0]
		}
	}
	return header
}

func addContentHeaderToHTTPHeader(nexusHeader nexus.Header, httpHeader http.Header) http.Header {
	for k, v := range nexusHeader {
		httpHeader.Set("Content-"+k, v)
	}
	return httpHeader
}

func addCallbackHeaderToHTTPHeader(nexusHeader nexus.Header, httpHeader http.Header) http.Header {
	for k, v := range nexusHeader {
		httpHeader.Set("Nexus-Callback-"+k, v)
	}
	return httpHeader
}

func addLinksToHTTPHeader(links []nexus.Link, httpHeader http.Header) error {
	for _, link := range links {
		encodedLink, err := encodeLink(link)
		if err != nil {
			return err
		}
		httpHeader.Add(headerLink, encodedLink)
	}
	return nil
}

func getLinksFromHeader(httpHeader http.Header) ([]nexus.Link, error) {
	var links []nexus.Link
	headerValues := httpHeader.Values(headerLink)
	if len(headerValues) == 0 {
		return nil, nil
	}
	for _, encodedLink := range strings.Split(strings.Join(headerValues, ","), ",") {
		link, err := decodeLink(encodedLink)
		if err != nil {
			return nil, err
		}
		links = append(links, link)
	}
	return links, nil
}

func httpHeaderToNexusHeader(httpHeader http.Header, excludePrefixes ...string) nexus.Header {
	header := nexus.Header{}
headerLoop:
	for k, v := range httpHeader {
		lowerK := strings.ToLower(k)
		for _, prefix := range excludePrefixes {
			if strings.HasPrefix(lowerK, prefix) {
				continue headerLoop
			}
		}
		// Nexus headers can only have single values, ignore multiple values.
		header[lowerK] = v[0]
	}
	return header
}

func addNexusHeaderToHTTPHeader(nexusHeader nexus.Header, httpHeader http.Header) http.Header {
	for k, v := range nexusHeader {
		httpHeader.Set(k, v)
	}
	return httpHeader
}

func addContextTimeoutToHTTPHeader(ctx context.Context, httpHeader http.Header) http.Header {
	deadline, ok := ctx.Deadline()
	if !ok {
		return httpHeader
	}
	httpHeader.Set(nexus.HeaderRequestTimeout, FormatDuration(time.Until(deadline)))
	return httpHeader
}

const linkTypeKey = "type"

// decodeLink encodes the link to Nexus-Link header value.
// It follows the same format of HTTP Link header: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Link
func encodeLink(link nexus.Link) (string, error) {
	if err := validateLinkURL(link.URL); err != nil {
		return "", fmt.Errorf("failed to encode link: %w", err)
	}
	if err := validateLinkType(link.Type); err != nil {
		return "", fmt.Errorf("failed to encode link: %w", err)
	}
	return fmt.Sprintf(`<%s>; %s="%s"`, link.URL.String(), linkTypeKey, link.Type), nil
}

// decodeLink decodes the Nexus-Link header values.
// It must have the same format of HTTP Link header: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Link
func decodeLink(encodedLink string) (nexus.Link, error) {
	var link nexus.Link
	encodedLink = strings.TrimSpace(encodedLink)
	if len(encodedLink) == 0 {
		return link, errors.New("failed to parse link header: value is empty")
	}

	if encodedLink[0] != '<' {
		return link, fmt.Errorf("failed to parse link header: invalid format: %s", encodedLink)
	}
	urlEnd := strings.Index(encodedLink, ">")
	if urlEnd == -1 {
		return link, fmt.Errorf("failed to parse link header: invalid format: %s", encodedLink)
	}
	urlStr := strings.TrimSpace(encodedLink[1:urlEnd])
	if len(urlStr) == 0 {
		return link, errors.New("failed to parse link header: url is empty")
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return link, fmt.Errorf("failed to parse link header: invalid url: %s", urlStr)
	}
	if err := validateLinkURL(u); err != nil {
		return link, fmt.Errorf("failed to parse link header: %w", err)
	}
	link.URL = u

	params := strings.Split(encodedLink[urlEnd+1:], ";")
	// must contain at least one semi-colon, and first param must be empty since
	// it corresponds to the url part parsed above.
	if len(params) < 2 {
		return link, fmt.Errorf("failed to parse link header: invalid format: %s", encodedLink)
	}
	if strings.TrimSpace(params[0]) != "" {
		return link, fmt.Errorf("failed to parse link header: invalid format: %s", encodedLink)
	}

	typeKeyFound := false
	for _, param := range params[1:] {
		param = strings.TrimSpace(param)
		if len(param) == 0 {
			return link, fmt.Errorf("failed to parse link header: parameter is empty: %s", encodedLink)
		}
		kv := strings.SplitN(param, "=", 2)
		if len(kv) != 2 {
			return link, fmt.Errorf("failed to parse link header: invalid parameter format: %s", param)
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		if strings.HasPrefix(val, `"`) != strings.HasSuffix(val, `"`) {
			return link, fmt.Errorf(
				"failed to parse link header: parameter value missing double-quote: %s",
				param,
			)
		}
		if strings.HasPrefix(val, `"`) {
			val = val[1 : len(val)-1]
		}
		if key == linkTypeKey {
			if err := validateLinkType(val); err != nil {
				return link, fmt.Errorf("failed to parse link header: %w", err)
			}
			link.Type = val
			typeKeyFound = true
		}
	}
	if !typeKeyFound {
		return link, fmt.Errorf(
			"failed to parse link header: %q key not found: %s",
			linkTypeKey,
			encodedLink,
		)
	}

	return link, nil
}

func validateLinkURL(value *url.URL) error {
	if value == nil || value.String() == "" {
		return errors.New("url is empty")
	}
	_, err := url.ParseQuery(value.RawQuery)
	if err != nil {
		return fmt.Errorf("url query not percent-encoded: %s", value)
	}
	return nil
}

func validateLinkType(value string) error {
	if len(value) == 0 {
		return errors.New("link type is empty")
	}
	for _, c := range value {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' && c != '.' && c != '/' {
			return errors.New("link type contains invalid char (valid chars: alphanumeric, '_', '.', '/')")
		}
	}
	return nil
}

var durationRegexp = regexp.MustCompile(`^(\d+(?:\.\d+)?)(ms|s|m)$`)

func ParseDuration(value string) (time.Duration, error) {
	m := durationRegexp.FindStringSubmatch(value)
	if len(m) == 0 {
		return 0, fmt.Errorf("invalid duration: %q", value)
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, err
	}

	switch m[2] {
	case "ms":
		return time.Millisecond * time.Duration(v), nil
	case "s":
		return time.Millisecond * time.Duration(v*1e3), nil
	case "m":
		return time.Millisecond * time.Duration(v*1e3*60), nil
	}
	// nolint:forbidigo // code is unreachable due to regex validation
	panic("unreachable")
}

// FormatDuration converts a duration into a string representation in millisecond resolution.
func FormatDuration(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
}
