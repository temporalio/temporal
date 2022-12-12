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
	opaAuthorizer struct {
		opaEndpoint string
	}
)

var _ Authorizer = (*opaAuthorizer)(nil)

// NewDefaultAuthorizer creates a default authorizer
func NewOpaAuthorizer(opaEndpoint string) Authorizer {
	return &opaAuthorizer{
		opaEndpoint: opaEndpoint,
	}
}

type (
	opaInput struct {
		Claims Claims
		Target CallTarget
	}

	opaRequest struct {
		Input opaInput `json:"input"`
	}

	opaResult struct {
		Result struct {
			Allow bool `json:"allow"`
		} `json:"result"`
	}
)

func (a *opaAuthorizer) Authorize(_ context.Context, claims *Claims, target *CallTarget) (Result, error) {
	opaRequest := opaRequest{
		Input: opaInput{
			Claims: *claims,
			Target: *target,
		},
	}

	jsonData, err := json.Marshal(opaRequest)
	if err != nil {
		return resultDeny, err
	}

	response, err := http.Post(a.opaEndpoint, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return resultDeny, err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return resultDeny, fmt.Errorf("OPA returned a status code %d", response.StatusCode)
	}

	var opaResult opaResult
	err = json.NewDecoder(response.Body).Decode(&opaRequest)
	if err != nil {
		return resultDeny, err
	}

	if opaResult.Result.Allow {
		return resultAllow, nil
	}

	return resultDeny, nil
}
