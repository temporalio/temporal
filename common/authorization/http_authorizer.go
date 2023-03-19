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

package authorization

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type (
	httpAuthorizer struct {
		httpEndpoint string
	}
)

var _ Authorizer = (*httpAuthorizer)(nil)

// NewDefaultAuthorizer creates a default authorizer
func NewHttpAuthorizer(httpEndpoint string) Authorizer {
	return &httpAuthorizer{
		httpEndpoint: httpEndpoint,
	}
}

type (
	httpInput struct {
		Claims Claims
		Target CallTarget
	}

	httpRequest struct {
		Input httpInput `json:"input"`
	}

	httpResult struct {
		Result struct {
			Allow bool `json:"allow"`
		} `json:"result"`
	}
)

func (a *httpAuthorizer) Authorize(ctx context.Context, claims *Claims, target *CallTarget) (Result, error) {
	httpRequest := httpRequest{
		Input: httpInput{
			Claims: *claims,
			Target: *target,
		},
	}

	jsonData, err := json.Marshal(httpRequest)
	if err != nil {
		return resultDeny, err
	}

	request, err := http.NewRequestWithContext(ctx, "POST", a.httpEndpoint, bytes.NewReader(jsonData))
	if err != nil {
		return resultDeny, err
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return resultDeny, err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return resultDeny, fmt.Errorf("OPA returned a status code %d", response.StatusCode)
	}

	var httpResult httpResult
	err = json.NewDecoder(response.Body).Decode(&httpResult)
	if err != nil {
		return resultDeny, err
	}

	if httpResult.Result.Allow {
		return resultAllow, nil
	}

	return resultDeny, nil
}
