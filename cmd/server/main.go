// Command server runs the realtime WebSocket hub.
//
//	go run ./cmd/server            # ws://localhost:8080/ws?room=lobby
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/Tajaddin/go-realtime-hub/internal/hub"
	"github.com/Tajaddin/go-realtime-hub/internal/wsserver"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP/WebSocket listen address")
	flag.Parse()

	h := hub.New()
	srv := &http.Server{
		Addr:              *addr,
		Handler:           wsserver.New(h).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("realtime hub listening on %s (ws path /ws?room=NAME)", *addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
