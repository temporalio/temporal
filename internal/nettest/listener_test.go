package nettest

import (
	"context"
	"fmt"
	"net"
	"net/http"
)

func ExampleListener() {
	pipe := NewPipe()
	listener := NewListener(pipe)
	server := http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("hello"))
			w.WriteHeader(200)
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
	var buf [5]byte
	_, _ = resp.Body.Read(buf[:])
	_ = server.Close()
	fmt.Println(string(buf[:]))
	// Output: hello
}
