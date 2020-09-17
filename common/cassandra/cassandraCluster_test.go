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

package cassandra

import (
	"errors"
	"testing"
	"encoding/base64"

	"github.com/stretchr/testify/assert"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/service/config"
)

func TestNewCassandraCluster(t *testing.T) {
	tests := map[string]struct {
		cfg config.Cassandra
		err error
	}{
		"emptyConfig": {
			cfg: config.Cassandra{},
			err: nil,
		},
		"badBase64": {
			cfg: config.Cassandra{
				TLS: &auth.TLS{ Enabled: true, CaData: "this isn't base64" },
			},
			err: base64.CorruptInputError(4),
		},
		"badPEM": {
			cfg: config.Cassandra{
				TLS: &auth.TLS{ Enabled: true, CaData: "dGhpcyBpc24ndCBhIFBFTSBjZXJ0" },
			},
			err: errors.New("failed to load decoded CA Cert as PEM"),
		},
		"withCaCert": {
			cfg: config.Cassandra{
				TLS: &auth.TLS{
					Enabled: true,
					CaData: "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURvRENDQW9pZ0F3SUJBZ0lVTDFKdUx0K0dRcWNuM0pDZGNiaUxibjBmSjhBd0RRWUpLb1pJaHZjTkFRRUwKQlFBd2FERUxNQWtHQTFVRUJoTUNWVk14RXpBUkJnTlZCQWdUQ2xkaGMyaHBibWQwYjI0eEVEQU9CZ05WQkFjVApCMU5sWVhSMGJHVXhEVEFMQmdOVkJBb1RCRlZ1YVhReERUQUxCZ05WQkFzVEJGUmxjM1F4RkRBU0JnTlZCQU1UCkMxVnVhWFJVWlhOMElFTkJNQjRYRFRJd01Ea3hOekUzTXpVd01Gb1hEVEkxTURreE5qRTNNelV3TUZvd2FERUwKTUFrR0ExVUVCaE1DVlZNeEV6QVJCZ05WQkFnVENsZGhjMmhwYm1kMGIyNHhFREFPQmdOVkJBY1RCMU5sWVhSMApiR1V4RFRBTEJnTlZCQW9UQkZWdWFYUXhEVEFMQmdOVkJBc1RCRlJsYzNReEZEQVNCZ05WQkFNVEMxVnVhWFJVClpYTjBJRU5CTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF0ZzU5SGU2MDVlYjIKcThGYUpycHBoRVNPZnJiVEdIQlhXRk41Z0N1QUlZMmVNTUdKSnFwWERrUjNGWko2TFZaYXFkVm9rWmkzeVRIOQprWW5uTEhBRDJJKzd5M0FnczB0WWZucmx0MGhtWjNleVlRSGk0Y1d0Vkd3aVoycW0yQnZMbzJVMENkeXRSSjRRCjNlQVQyeVRrTnZ4Wm9XeUhHK09icjZ4UFByMjh2bWo3Q0txVnNLQ0FIVnlqdXlybXRJcHdkbWVpVTlFbTFTTUgKSVBLR0pJQ29NeGl4NXNDdHVqZmRSTWJTU2hIRFluUmdmMkx2enIxVk5mZkdaS01YekJaekkyZ3BJZm9YaGZVUwpkdmNlUTVoWXo4emdEY2hDOG1laEM3bU12Myt6Q3d6OWtGbmJpYnBvSVdGcStGbzYzeHNnc255dFlQTXY0cmltClgwSWRwZlA2VVFJREFRQUJvMEl3UURBT0JnTlZIUThCQWY4RUJBTUNBUVl3RHdZRFZSMFRBUUgvQkFVd0F3RUIKL3pBZEJnTlZIUTRFRmdRVWUzY0MvMllrTWRmRUZUbUliMW84M1U0VWgxY3dEUVlKS29aSWh2Y05BUUVMQlFBRApnZ0VCQUN2TTVURG9BM0FFRFlrcFlueWwwVlpmRDdKRVhWSEJ5WTEyeG9jUHM4TGJzNEtNS1NtUGVld0dIU25WCisrQVdFdG8vWlFjUnVVcm9SR2ZFRDRTU3kyT0tyNGh4M0J0UmNGRkZrdFg4U2Uwck5rSitaSHVoVFBWdWQ5L00KUXRBenl2UWVkcDBXQlcydDBvWkhDcVNOWmMvSWFYWGNxeTdocHpLOHBLZTNOUXYyUkdHVkEybWRDR1oxUE5rMgpFTXhMVnhoUURkbTNKRWJ4SEJPNCtVWm45MHVDd1BGc25rVFFmNm53WTErMjNMc3lheFkxWXFkeXZHTzhjdEc0CnozUmRTbTVJM25XaTNERFd3TnhuZ1lpMCtBL01VQ0FBYjBOejluSXI3dzB5UlJpWHJha1hUYjlaOC9GWE1JdHEKdG5wckJzK3hhYzhBVWxzcEw3cCtUWmRUMFdNPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==",
				},
			},
			err: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := NewCassandraCluster(tc.cfg)
			if !errors.Is(err, tc.err) {
				assert.Equal(t, tc.err, err)
			}
		})
	}
}
