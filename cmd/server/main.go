// Command server runs the realtime WebSocket hub.
//
//	go run ./cmd/server            # ws://localhost:8080/ws?room=lobby
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Tajaddin/go-realtime-hub/internal/hub"
	"github.com/Tajaddin/go-realtime-hub/internal/wsserver"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP/WebSocket listen address")
	origins := flag.String("allowed-origins", "", "comma-separated browser origins allowed to open a WebSocket besides the server's own, e.g. https://app.example.com")
	flag.Parse()

	var allowed []string
	for _, o := range strings.Split(*origins, ",") {
		if o = strings.TrimSpace(o); o != "" {
			allowed = append(allowed, o)
		}
	}

	h := hub.New()
	srv := &http.Server{
		Addr:              *addr,
		Handler:           wsserver.New(h, allowed...).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("realtime hub listening on %s (ws path /ws?room=NAME)", *addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
