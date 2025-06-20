package nettest_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

	"go.temporal.io/server/common/testing/nettest"
)

func ExampleListener() {
	pipe := nettest.NewPipe()
	listener := nettest.NewListener(pipe)
	server := http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("hello"))
		}),
	}

	go func() {
		_ = server.Serve(listener)
	}()

	client := http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return pipe.Connect(ctx.Done())
			},
		},
	}
	resp, _ := client.Get("http://fake")

	defer func() {
		_ = resp.Body.Close()
	}()

	buf, _ := io.ReadAll(resp.Body)
	_ = server.Close()

	fmt.Println(string(buf[:]))
	// Output: hello
}
