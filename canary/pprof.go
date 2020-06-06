package canary

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
)

func StartPProf(port int) {
	go func() {
		err := http.ListenAndServe(fmt.Sprintf("localhost:%d", port), nil)
		if err != nil {
			log.Printf("Unable to start profiler on port %d: %v", port, err)
		}
	}()
}
